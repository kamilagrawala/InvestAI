import pika
import json
import logging
import os
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

class DayTraderAgent:
    def __init__(self):
        # State: trades[account_number][ticker][date] = count
        self.trade_history = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        
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
        
        # Track the trade
        self.trade_history[account][ticker][date_str] += 1
        count = self.trade_history[account][ticker][date_str]
        
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

    def start(self):
        rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        rabbitmq_user = os.getenv('RABBITMQ_USER', 'admin')
        rabbitmq_pass = os.getenv('RABBITMQ_PASS', 'password')
        
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=rabbitmq_host,
            credentials=credentials
        ))
        channel = connection.channel()

        # Create a dedicated queue for the audit agent
        channel.queue_declare(queue='audit_trades', durable=True)
        
        # Bind it to the SAME exchange and routing key as the main consumers
        # This creates a "copy" of the message for the agent (Fanout-like behavior with direct exchange)
        channel.queue_bind(exchange='amq.direct', queue='audit_trades', routing_key='stock_trades')

        def callback(ch, method, properties, body):
            trade_data = json.loads(body)
            self.process_trade(trade_data)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue='audit_trades', on_message_callback=callback)
        
        logger.info(f"Audit Agent active. Monitoring trades via audit_trades queue...")
        channel.start_consuming()

if __name__ == "__main__":
    agent = DayTraderAgent()
    agent.start()
