import pika
import json
import logging
import os
import redis
import time
import threading
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from crypto_utils import decrypt_string

# Force load local environment
load_dotenv(override=True)

from langchain.prompts import PromptTemplate
from langchain_community.llms import FakeListLLM
from langchain_google_genai import ChatGoogleGenerativeAI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AuditAgent")

# --- Structured Output Schema ---
class FlaggedAccount(BaseModel):
    account: str = Field(description="The ID of the account being audited")
    decision: str = Field(description="Either FLAG or PASS")
    reason: str = Field(description="Brief explanation of the decision")

class AuditReport(BaseModel):
    flagged_accounts: List[FlaggedAccount] = Field(description="List of accounts that met the PDT criteria")

# --- Agent Implementation ---
class AuditAgent:
    def __init__(self):
        # [DEBUG]
        self.current_provider = os.getenv("LLM_PROVIDER", "NOT_SET")
        print(f"\n[DEBUG AUDIT] PROVISIONING AGENT WITH PROVIDER: {self.current_provider}\n", flush=True)
        
        # 1. Redis setup for shared memory
        redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
        self.history_limit = 20
        self.audit_window_seconds = 60 # Global 60s audit window
        
        # 2. LLM Factory Setup (Pure LangChain)
        self.llm = self._get_llm(self.current_provider)
        
        # 3. The "Global PDT Expert" Prompt
        self.prompt = PromptTemplate.from_template(
            "SYSTEM: You are a Financial Compliance Officer. Your task is to identify PATTERN DAY TRADERS (PDT).\n"
            "DEFINITION: A Pattern Day Trader executes four or more 'round-trip' trades "
            "within five business days. A 'round-trip' is the purchase and sale (or sale and purchase) "
            "of the same security on the same day.\n\n"
            "CONTEXT: Here is the recent trade history for multiple accounts:\n"
            "{batch_data}\n\n"
            "ANALYSIS: For each account, analyze if they meet the PDT criteria.\n"
        )
        
        # Enable Structured Output if using a real model
        if self.current_provider == "gemini":
            self.chain = self.prompt | self.llm.with_structured_output(AuditReport)
        else:
            # Fallback for FakeLLM which doesn't support with_structured_output
            self.chain = self.prompt | self.llm

        # 4. Enable Keyspace Notifications
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
            # Realistic stub matching the schema
            mock_json = {
                "flagged_accounts": [
                    {"account": "ACC_0", "decision": "FLAG", "reason": "Meets PDT threshold."},
                    {"account": "ACC_1", "decision": "FLAG", "reason": "High frequency round-trips."}
                ]
            }
            return FakeListLLM(responses=[json.dumps(mock_json)])
        elif provider == "gemini":
            api_key_raw = os.getenv("GEMINI_API_KEY")
            api_key = decrypt_string(api_key_raw, env_name="GOOGLE_MASTER_KEY")
            
            logger.info("Connecting to Gemini via LangChain (gemini-flash-latest)...")
            return ChatGoogleGenerativeAI(
                model="gemini-flash-latest",
                google_api_key=api_key,
                temperature=0,
                max_tokens=2048,
                timeout=30
            )
        return FakeListLLM(responses=['{"flagged_accounts": []}'])

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

    def _publish_notification(self, notification_event):
        """Helper to publish notifications using a fresh connection to be thread-safe."""
        try:
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
            channel = connection.channel()
            
            channel.basic_publish(
                exchange='amq.direct',
                routing_key='notifications',
                body=json.dumps(notification_event),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            connection.close()
            logger.info("Successfully published consolidated notification.")
        except Exception as e:
            logger.error(f"Failed to publish notification: {e}")

    def _perform_batch_ai_audit(self):
        """Performs a single AI call for all accounts that traded in the last window."""
        active_provider = os.getenv("LLM_PROVIDER", "unknown")
        try:
            active_accounts = self.redis.smembers("active_accounts_this_window")
            if not active_accounts: return

            batch_data = ""
            for account in active_accounts:
                history_key = f"history:{account}"
                history_list = self.redis.lrange(history_key, 0, -1)
                batch_data += f"\nACCOUNT: {account}\n"
                batch_data += "\n".join([f"- {item}" for item in history_list])
                batch_data += "\n" + ("=" * 20)

            logger.info(f" [GLOBAL AUDIT] Window closed. Analyzing {len(active_accounts)} accounts in ONE call...")
            
            try:
                # Execute LangChain Chain (Real AI returns Pydantic object, Fake returns string)
                result = self.chain.invoke({"batch_data": batch_data})
                
                # Normalize result to dict
                if hasattr(result, 'model_dump'): # If Pydantic model (Real AI)
                    flagged_list = result.model_dump().get('flagged_accounts', [])
                else: # If string (Fake LLM)
                    flagged_list = json.loads(result).get('flagged_accounts', [])
                
                if flagged_list:
                    # Filter only those with DECISION: FLAG for the alert log
                    actually_flagged = [a for a in flagged_list if a.get('decision') == 'FLAG']
                    
                    if actually_flagged:
                        logger.warning(f"!!! GLOBAL ALERT !!! AI flagged {len(actually_flagged)} accounts.")
                    
                    print(f"\n[AI REASONING]\n{json.dumps(flagged_list, indent=2)}\n", flush=True)
                    
                    notification_event = {
                        "event_type": "BATCH_PDT_ALERT",
                        "flagged_accounts": flagged_list,
                        "total_audited_count": len(active_accounts), # Explicitly pass total
                        "provider": active_provider,
                        "timestamp": datetime.now().isoformat()
                    }
                    print(f"[DEBUG AUDIT] PUBLISHING ALERT WITH PROVIDER: {active_provider}", flush=True)
                    self._publish_notification(notification_event)
                else:
                    logger.info("Global Audit: No accounts flagged this window.")

            except Exception as e:
                logger.error(f"LLM Call or Parsing Failed: {e}")
                notification_event = {
                    "event_type": "BATCH_PDT_ALERT",
                    "flagged_accounts": [{"account": "SYSTEM", "decision": "ERROR", "reason": str(e)[:100]}],
                    "provider": f"ERROR ({active_provider})",
                    "timestamp": datetime.now().isoformat()
                }
                self._publish_notification(notification_event)

            self.redis.delete("active_accounts_this_window")
                
        except Exception as e:
            logger.error(f"Error during global batch audit: {e}")

    def process_trade(self, trade_data):
        account = trade_data.get('Account Number')
        ticker = trade_data.get('Ticker')
        action = trade_data.get('Action')
        trade_id = trade_data.get('TradeID', 'N/A')
        ts = trade_data.get('Date', datetime.now().isoformat())
        
        history_key = f"history:{account}"
        trade_entry = f"{ts} | {action} | {ticker} | ID:{trade_id}"
        self.redis.rpush(history_key, trade_entry)
        self.redis.ltrim(history_key, -self.history_limit, -1)
        self.redis.expire(history_key, 604800)

        self.redis.sadd("active_accounts_this_window", account)

        timer_key = "global_audit_timer"
        if not self.redis.get(timer_key):
            self.redis.setex(timer_key, self.audit_window_seconds, "active")
            logger.info(f" [GLOBAL AUDIT] Started 60s global window.")

    def start(self):
        rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        rabbitmq_user_raw = os.getenv('RABBITMQ_USER', 'admin')
        rabbitmq_pass_raw = os.getenv('RABBITMQ_PASS', 'password')
        
        max_retries = 10
        for attempt in range(max_retries):
            try:
                try:
                    r_user = decrypt_string(rabbitmq_user_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_user_raw) > 50 else rabbitmq_user_raw
                    r_pass = decrypt_string(rabbitmq_pass_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_pass_raw) > 50 else rabbitmq_pass_raw
                except Exception:
                    r_user, r_pass = rabbitmq_user_raw, rabbitmq_pass_raw

                credentials = pika.PlainCredentials(r_user, r_pass)
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
                self.channel = connection.channel()
                break
            except Exception as e:
                logger.warning(f"RabbitMQ not ready (attempt {attempt+1}/{max_retries})...")
                time.sleep(5)
        else:
            logger.error("Failed to connect to RabbitMQ after retries.")
            return

        self.channel.queue_declare(queue='audit_trades', durable=True)
        self.channel.queue_bind(exchange='amq.direct', queue='audit_trades', routing_key='stock_trades')
        self.channel.queue_declare(queue='notifications', durable=True)
        self.channel.queue_bind(exchange='amq.direct', queue='notifications', routing_key='notifications')

        def callback(ch, method, properties, body):
            trade_data = json.loads(body)
            self.process_trade(trade_data)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue='audit_trades', on_message_callback=callback)
        logger.info(f"Global PDT Audit Agent active. Provider: {self.current_provider}")
        self.channel.start_consuming()

if __name__ == "__main__":
    agent = AuditAgent()
    agent.start()
