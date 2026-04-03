import pika
import json
import logging
import os
import redis
from crypto_utils import decrypt_string
from datetime import datetime
from collections import defaultdict
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_community.llms import FakeListLLM # Using a fake LLM for logic flow without API keys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AuditAgent")

class AuditAgent:
    def __init__(self):
        # Redis setup for shared state across agents
        redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)

        # Setup a simple LangChain logic (using Fake LLM for now)
        # In production, this would use an LLM to analyze complex patterns
        self.prompt = PromptTemplate.from_template(
            "Analyze trade activity: Account {account} made {count} {action} trades for {ticker} today. "
            "Is this a day trader?"
        )
        self.llm = FakeListLLM(responses=["The account owner is likely a day trader based on high frequency activity."])
        self.chain = self.prompt | self.llm | StrOutputParser()

    def process_trade(self, trade_data):
        account = trade_data.get('Account Number')
        ticker = trade_data.get('Ticker')
        action = trade_data.get('Action')
        date_str = trade_data.get('Date', '').split('T')[0] # Get YYYY-MM-DD
        
        # Redis key for this specific account, stock, and day
        # Format: trades:ACC_1:AAPL:2026-04-03
        count_key = f"trades:{account}:{ticker}:{date_str}"
        
        # Increment the shared counter in Redis (Atomic operation)
        count = self.redis.incr(count_key)
        
        # Ensure the key expires after 24 hours to keep Redis clean
        if count == 1:
            self.redis.expire(count_key, 86400)
        
        # Day Trader Detection Logic: 
        # Pattern: Same account, same stock, multiple times in one day
        if count >= 3:
            # Trigger LangChain Agent Analysis
            analysis = self.chain.invoke({
                "account": account,
                "count": count,
                "action": action,
                "ticker": ticker
            })
            
            logger.warning(f"!!! ALERT !!! Account {account} flagged. Pattern: {count} trades for {ticker} on {date_str}.")
            logger.warning(f"Agent Analysis: {analysis}")

            # NEW: Publish to notification queue
            notification_event = {
                "event_type": "DAY_TRADER_ALERT",
                "account_number": account,
                "ticker": ticker,
                "trade_count": count,
                "timestamp": datetime.now().isoformat(),
                "details": analysis
            }
            self.channel.basic_publish(
                exchange='amq.direct',
                routing_key='notifications',
                body=json.dumps(notification_event),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.info(f"Published notification event for {account}")

    def start(self):
        rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        rabbitmq_user_raw = os.getenv('RABBITMQ_USER', 'admin')
        rabbitmq_pass_raw = os.getenv('RABBITMQ_PASS', 'password')
        
        # Decrypt credentials if they look like encrypted strings
        try:
            # Decrypt User
            if len(rabbitmq_user_raw) > 50:
                rabbitmq_user = decrypt_string(rabbitmq_user_raw, env_name="RABBITMQ_MASTER_KEY")
            else:
                rabbitmq_user = rabbitmq_user_raw
                
            # Decrypt Password
            if len(rabbitmq_pass_raw) > 50:
                rabbitmq_pass = decrypt_string(rabbitmq_pass_raw, env_name="RABBITMQ_MASTER_KEY")
            else:
                rabbitmq_pass = rabbitmq_pass_raw
        except Exception:
            rabbitmq_user = rabbitmq_user_raw
            rabbitmq_pass = rabbitmq_pass_raw

        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=rabbitmq_host,
            credentials=credentials
        ))
        self.channel = connection.channel()

        # Create a dedicated queue for the audit agent
        self.channel.queue_declare(queue='audit_trades', durable=True)
        
        # Bind it to the SAME exchange and routing key as the main consumers
        self.channel.queue_bind(exchange='amq.direct', queue='audit_trades', routing_key='stock_trades')

        # NEW: Ensure the notifications queue exists
        self.channel.queue_declare(queue='notifications', durable=True)
        self.channel.queue_bind(exchange='amq.direct', queue='notifications', routing_key='notifications')

        def callback(ch, method, properties, body):
            trade_data = json.loads(body)
            self.process_trade(trade_data)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue='audit_trades', on_message_callback=callback)
        
        logger.info(f"Audit Agent active. Monitoring trades via audit_trades queue...")
        self.channel.start_consuming()

if __name__ == "__main__":
    agent = AuditAgent()
    agent.start()
