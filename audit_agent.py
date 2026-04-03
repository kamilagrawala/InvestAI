import pika
import json
import logging
import os
import redis
import time
from datetime import datetime
from crypto_utils import decrypt_string
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_community.llms import FakeListLLM

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AuditAgent")

class AuditAgent:
    def __init__(self):
        # 1. Redis setup for shared memory
        redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
        self.history_limit = 20 # Keep last 20 trades for PDT analysis
        
        # 2. LLM Factory Setup
        provider = os.getenv("LLM_PROVIDER", "fake").lower()
        self.llm = self._get_llm(provider)
        
        # 3. The "PDT Expert" Prompt
        self.prompt = PromptTemplate.from_template(
            "SYSTEM: You are a Financial Compliance Officer. Your task is to identify PATTERN DAY TRADERS (PDT).\n"
            "DEFINITION: A Pattern Day Trader is an account that executes four or more 'round-trip' trades "
            "within five business days. A 'round-trip' is the purchase and sale (or sale and purchase) "
            "of the same security on the same day.\n\n"
            "CONTEXT: Recent trade history for Account {account}:\n"
            "{history}\n\n"
            "ANALYSIS: Look for round-trips. Count them. \n"
            "If the account meets the PDT criteria, respond with 'DECISION: FLAG'.\n"
            "If not, respond with 'DECISION: PASS'.\n"
            "Include your reasoning after the decision.\n"
        )
        self.chain = self.prompt | self.llm | StrOutputParser()

    def _get_llm(self, provider):
        if provider == "fake":
            # Stubbed responses for different scenarios
            return FakeListLLM(responses=[
                "The account made 5 round-trips in 2 days. DECISION: FLAG. REASON: Meets PDT threshold.",
                "Only 1 trade detected. DECISION: PASS. REASON: No round-trips found.",
                "Multiple buys but no sells today. DECISION: PASS. REASON: Not a round-trip."
            ])
        # Add real providers (OpenAI, etc.) here in the future
        return FakeListLLM(responses=["DECISION: PASS. REASON: Provider not configured."])

    def process_trade(self, trade_data):
        account = trade_data.get('Account Number')
        ticker = trade_data.get('Ticker')
        action = trade_data.get('Action')
        trade_id = trade_data.get('TradeID', 'N/A')
        ts = trade_data.get('Date', datetime.now().isoformat())
        
        # 1. Update Short-Term Memory in Redis (Rolling Window)
        history_key = f"history:{account}"
        trade_entry = f"{ts} | {action} | {ticker} | ID:{trade_id}"
        
        self.redis.rpush(history_key, trade_entry)
        self.redis.ltrim(history_key, -self.history_limit, -1)
        self.redis.expire(history_key, 604800)

        # 2. Retrieve Full Context
        history_list = self.redis.lrange(history_key, 0, -1)
        history_text = "\n".join([f"- {item}" for item in history_list])

        # 3. Intelligence Phase: PDT Analysis
        logger.info(f"Analyzing PDT risk for {account}...")
        
        # In production, this uses the real LLM chain. 
        # In fake mode, it cycles through stubbed responses.
        analysis = self.chain.invoke({
            "account": account,
            "history": history_text
        })

        # 4. Action Phase
        if "DECISION: FLAG" in analysis:
            logger.warning(f"!!! PDT ALERT !!! Account {account} flagged by AI.")
            logger.warning(f"Reasoning: {analysis}")

            notification_event = {
                "event_type": "PATTERN_DAY_TRADER_ALERT",
                "account_number": account,
                "ticker": "MULTIPLE",
                "trade_count": len(history_list),
                "timestamp": datetime.now().isoformat(),
                "details": analysis
            }
            self.channel.basic_publish(
                exchange='amq.direct',
                routing_key='notifications',
                body=json.dumps(notification_event),
                properties=pika.BasicProperties(delivery_mode=2)
            )
        else:
            logger.info(f"Account {account} passed PDT check.")

    def start(self):
        rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        rabbitmq_user_raw = os.getenv('RABBITMQ_USER', 'admin')
        rabbitmq_pass_raw = os.getenv('RABBITMQ_PASS', 'password')
        
        try:
            if len(rabbitmq_user_raw) > 50:
                rabbitmq_user = decrypt_string(rabbitmq_user_raw, env_name="RABBITMQ_MASTER_KEY")
            else:
                rabbitmq_user = rabbitmq_user_raw
            if len(rabbitmq_pass_raw) > 50:
                rabbitmq_pass = decrypt_string(rabbitmq_pass_raw, env_name="RABBITMQ_MASTER_KEY")
            else:
                rabbitmq_pass = rabbitmq_pass_raw
        except Exception:
            rabbitmq_user = rabbitmq_user_raw
            rabbitmq_pass = rabbitmq_pass_raw

        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
        self.channel = connection.channel()

        self.channel.queue_declare(queue='audit_trades', durable=True)
        self.channel.queue_bind(exchange='amq.direct', queue='audit_trades', routing_key='stock_trades')
        self.channel.queue_declare(queue='notifications', durable=True)
        self.channel.queue_bind(exchange='amq.direct', queue='notifications', routing_key='notifications')

        def callback(ch, method, properties, body):
            trade_data = json.loads(body)
            self.process_trade(trade_data)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue='audit_trades', on_message_callback=callback)
        logger.info(f"PDT Audit Agent active. Using '{os.getenv('LLM_PROVIDER', 'fake')}' intelligence.")
        self.channel.start_consuming()

if __name__ == "__main__":
    agent = AuditAgent()
    agent.start()
