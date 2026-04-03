import pika
import json
import logging
import os
import redis
import time
import threading
from datetime import datetime
from crypto_utils import decrypt_string
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_community.llms import FakeListLLM
from langchain_google_genai import ChatGoogleGenerativeAI

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
        self.history_limit = 20
        self.audit_window_seconds = 60 # Global 60s audit window
        
        # 2. LLM Factory Setup
        provider = os.getenv("LLM_PROVIDER", "fake").lower()
        self.llm = self._get_llm(provider)
        
        # 3. The "Global PDT Expert" Prompt (Batched)
        self.prompt = PromptTemplate.from_template(
            "SYSTEM: You are a Financial Compliance Officer. Your task is to identify PATTERN DAY TRADERS (PDT).\n"
            "DEFINITION: A Pattern Day Trader executes four or more 'round-trip' trades "
            "within five business days. A 'round-trip' is the purchase and sale (or sale and purchase) "
            "of the same security on the same day.\n\n"
            "CONTEXT: Here is the recent trade history for multiple accounts:\n"
            "{batch_data}\n\n"
            "ANALYSIS: For each account, analyze if they meet the PDT criteria.\n"
            "RESPONSE: You MUST respond with a JSON list of flagged accounts only. \n"
            "Format: [ {{\"account\": \"ACC_ID\", \"decision\": \"FLAG\", \"reason\": \"...\"}}, ... ]\n"
            "If no accounts are flagged, respond with an empty list: []\n"
        )
        self.chain = self.prompt | self.llm | StrOutputParser()

        # 4. Enable Keyspace Notifications for Expiration events
        try:
            self.redis.config_set('notify-keyspace-events', 'Ex')
            logger.info("Redis keyspace notifications enabled for Global AuditAgent.")
        except Exception as e:
            logger.warning(f"Could not enable Redis notifications: {e}")

        # Start the background Global Expiration Listener
        self.listener_thread = threading.Thread(target=self._global_audit_listener, daemon=True)
        self.listener_thread.start()

    def _get_llm(self, provider):
        if provider == "fake":
            # Stubbed response for a batch of accounts
            return FakeListLLM(responses=[
                '[{"account": "ACC_0", "decision": "FLAG", "reason": "Meets PDT threshold."}, {"account": "ACC_1", "decision": "FLAG", "reason": "High frequency round-trips."}]',
                '[]' # No one flagged in next window
            ])
        elif provider == "gemini":
            api_key_raw = os.getenv("GEMINI_API_KEY")
            api_key = decrypt_string(api_key_raw, env_name="GOOGLE_MASTER_KEY")
            return ChatGoogleGenerativeAI(
                model="gemini-2.0-flash",
                google_api_key=api_key,
                temperature=0,
                max_tokens=1024 # Larger tokens for batch response
            )
        return FakeListLLM(responses=["[]"])

    def _global_audit_listener(self):
        """Waits for the Global Audit Timer to expire."""
        pubsub = self.redis.pubsub()
        pubsub.psubscribe("__keyevent@0__:expired")
        logger.info("Global Audit listener started.")
        
        for message in pubsub.listen():
            if message['type'] == 'pmessage':
                expired_key = message['data']
                if expired_key == "global_audit_timer":
                    self._perform_batch_ai_audit()

    def _perform_batch_ai_audit(self):
        """Performs a single AI call for all accounts that traded in the last window."""
        try:
            # 1. Get all accounts that traded
            active_accounts = self.redis.smembers("active_accounts_this_window")
            if not active_accounts:
                return

            # 2. Build the batch context
            batch_data = ""
            for account in active_accounts:
                history_key = f"history:{account}"
                history_list = self.redis.lrange(history_key, 0, -1)
                batch_data += f"\nACCOUNT: {account}\n"
                batch_data += "\n".join([f"- {item}" for item in history_list])
                batch_data += "\n" + ("=" * 20)

            logger.info(f" [GLOBAL AUDIT] Window closed. Analyzing {len(active_accounts)} accounts in ONE call...")
            
            # 3. Single Intelligence Phase
            analysis_json = self.chain.invoke({"batch_data": batch_data})
            
            # 4. Action Phase: Publish ONE consolidated notification
            try:
                flagged_accounts = json.loads(analysis_json)
                if flagged_accounts:
                    logger.warning(f"!!! GLOBAL ALERT !!! AI flagged {len(flagged_accounts)} accounts.")
                    
                    notification_event = {
                        "event_type": "BATCH_PDT_ALERT",
                        "flagged_accounts": flagged_accounts,
                        "timestamp": datetime.now().isoformat()
                    }
                    self.channel.basic_publish(
                        exchange='amq.direct',
                        routing_key='notifications',
                        body=json.dumps(notification_event),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                else:
                    logger.info("Global Audit: No accounts flagged this window.")
            except Exception as e:
                logger.error(f"Failed to parse AI JSON response: {e}. Raw response: {analysis_json}")

            # 5. Cleanup for next window
            self.redis.delete("active_accounts_this_window")
                
        except Exception as e:
            logger.error(f"Error during global batch audit: {e}")

    def process_trade(self, trade_data):
        account = trade_data.get('Account Number')
        ticker = trade_data.get('Ticker')
        action = trade_data.get('Action')
        trade_id = trade_data.get('TradeID', 'N/A')
        ts = trade_data.get('Date', datetime.now().isoformat())
        
        # 1. Update Memory
        history_key = f"history:{account}"
        trade_entry = f"{ts} | {action} | {ticker} | ID:{trade_id}"
        self.redis.rpush(history_key, trade_entry)
        self.redis.ltrim(history_key, -self.history_limit, -1)
        self.redis.expire(history_key, 604800)

        # 2. Track account in current window
        self.redis.sadd("active_accounts_this_window", account)

        # 3. Check/Start Global Timer
        timer_key = "global_audit_timer"
        if not self.redis.get(timer_key):
            self.redis.setex(timer_key, self.audit_window_seconds, "active")
            logger.info(f" [GLOBAL AUDIT] Started 60s global window.")

    def start(self):
        # RabbitMQ setup logic (same as before)
        rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        rabbitmq_user_raw = os.getenv('RABBITMQ_USER', 'admin')
        rabbitmq_pass_raw = os.getenv('RABBITMQ_PASS', 'password')
        
        try:
            r_user = decrypt_string(rabbitmq_user_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_user_raw) > 50 else rabbitmq_user_raw
            r_pass = decrypt_string(rabbitmq_pass_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_pass_raw) > 50 else rabbitmq_pass_raw
        except Exception:
            r_user, r_pass = rabbitmq_user_raw, rabbitmq_pass_raw

        credentials = pika.PlainCredentials(r_user, r_pass)
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
        logger.info(f"Global PDT Audit Agent active.")
        self.channel.start_consuming()

if __name__ == "__main__":
    agent = AuditAgent()
    agent.start()
