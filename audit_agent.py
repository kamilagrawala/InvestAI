import pika
import json
import logging
import os
import redis
import time
import threading
import psycopg2
from datetime import datetime, timedelta
from typing import List, Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from crypto_utils import decrypt_string

# Force load local environment
load_dotenv(override=True)

from langchain_core.prompts import PromptTemplate
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
    decision: str = Field(description="Either FLAG, PASS, or BLOCK")
    violation_type: str = Field(description="The type of activity detected: PDT, INSIDER_TRADING, SPOOFING, PUMP_AND_DUMP, UNAUTHORIZED, or NONE")
    severity: str = Field(description="Severity: LOW, MEDIUM, HIGH, CRITICAL")
    reason: str = Field(description="Detailed explanation of the decision and detected pattern")

class AuditReport(BaseModel):
    flagged_accounts: List[FlaggedAccount] = Field(description="List of accounts analyzed for compliance")

# --- Agent Implementation ---
class AuditAgent:
    def __init__(self):
        # [DEBUG]
        self.current_provider = os.getenv("LLM_PROVIDER", "NOT_SET")
        print(f"\n[DEBUG AUDIT] PROVISIONING AGENT WITH PROVIDER: {self.current_provider}\n", flush=True)
        
        # 1. Redis setup for shared memory
        redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
        self.history_limit = 50 # Increased limit for rehydrated history
        self.audit_window_seconds = 60 # Global 60s audit window
        
        # 2. DB credentials
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_name = os.getenv('DB_NAME', 'investai')
        self.db_user_enc = os.getenv('DB_USER_ENCRYPTED')
        self.db_pass_enc = os.getenv('DB_PASS_ENCRYPTED')

        # 3. LLM Factory Setup (Pure LangChain)
        self.llm = self._get_llm(self.current_provider)
        
        # 4. The "Global Compliance Auditor" Prompt
        self.prompt = PromptTemplate.from_template(
            "SYSTEM: You are a Senior Financial Compliance Auditor. Your task is to identify dubious or fraudulent trading activity.\n\n"
            "RULES TO ENFORCE:\n"
            "1. PATTERN DAY TRADER (PDT): Executes 4+ 'round-trip' trades (buy/sell or sell/buy of same security on same day) within 5 business days.\n"
            "2. WASH TRADING / SPOOFING: Rapidly buying and selling (or bidding/offering) to create artificial volume or move prices (e.g. 10+ trades in 10s).\n"
            "3. PUMP & DUMP: Coordinated rapid buying of low-cap stocks followed by a dump.\n"
            "4. INSIDER TRADING: Massive trades that appear suspiciously timed (e.g. outlier volume/price in single trade).\n\n"
            "CONTEXT: Here is the trade history for multiple accounts:\n"
            "{batch_data}\n\n"
            "ANALYSIS: For each account, analyze the patterns. \n"
            "- If activity is highly suspicious (SPOOFING, PUMP_AND_DUMP, INSIDER), set decision to 'BLOCK' and severity to 'HIGH' or 'CRITICAL'.\n"
            "- If it's a standard PDT violation, set decision to 'FLAG' and severity to 'MEDIUM'.\n"
            "- If no violation, set decision to 'PASS' and violation_type to 'NONE'.\n\n"
            "IMPORTANT: Keep 'reason' concise (under 200 characters). Do not truncate the account ID.\n"
        )
        
        # Enable Structured Output if using a real model
        if self.current_provider == "gemini":
            self.chain = self.prompt | self.llm.with_structured_output(AuditReport)
        else:
            # Fallback for FakeLLM which doesn't support with_structured_output
            self.chain = self.prompt | self.llm

        # 5. Enable Keyspace Notifications
        try:
            self.redis.config_set('notify-keyspace-events', 'Ex')
            logger.info("Redis keyspace notifications enabled for Global AuditAgent.")
        except Exception as e:
            logger.warning(f"Could not enable Redis notifications: {e}")

        # Start the background Global Expiration Listener
        self.listener_thread = threading.Thread(target=self._global_audit_listener, daemon=True)
        self.listener_thread.start()

    def _get_db_connection(self):
        try:
            db_user = decrypt_string(self.db_user_enc, env_name="POSTGRES_MASTER_KEY")
            db_pass = decrypt_string(self.db_pass_enc, env_name="POSTGRES_MASTER_KEY")
            return psycopg2.connect(host=self.db_host, database=self.db_name, user=db_user, password=db_pass)
        except Exception as e:
            logger.error(f"DB Connection failed in AuditAgent: {e}")
            return None

    def _get_history_from_db(self, account: str) -> List[str]:
        """Fetches last 5 days of history from Postgres for an account."""
        conn = self._get_db_connection()
        history = []
        if conn:
            try:
                cur = conn.cursor()
                five_days_ago = datetime.now() - timedelta(days=5)
                cur.execute("""
                    SELECT trade_date, action, ticker, trade_id 
                    FROM TRADEORDER 
                    WHERE account_number = %s AND trade_date >= %s
                    ORDER BY trade_date ASC
                """, (account, five_days_ago))
                for row in cur.fetchall():
                    ts, action, ticker, trade_id = row
                    history.append(f"{ts.isoformat()} | {action} | {ticker} | ID:{trade_id}")
                cur.close()
            except Exception as e:
                logger.error(f"Failed to fetch history from DB for {account}: {e}")
            finally:
                conn.close()
        return history

    def _get_llm(self, provider):
        if provider == "fake":
            # Realistic stub matching the expanded schema
            mock_json = {
                "flagged_accounts": [
                    {"account": "ACC_0", "decision": "FLAG", "violation_type": "PDT", "severity": "MEDIUM", "reason": "Meets PDT threshold."},
                    {"account": "ACC_1", "decision": "FLAG", "violation_type": "PDT", "severity": "MEDIUM", "reason": "High frequency round-trips."},
                    {"account": "ACC_SPOOF", "decision": "BLOCK", "violation_type": "SPOOFING", "severity": "HIGH", "reason": "Detected rapid buy-side activity followed by a large sell-side execution."},
                    {"account": "ACC_PUMP", "decision": "BLOCK", "violation_type": "PUMP_AND_DUMP", "severity": "CRITICAL", "reason": "Rapid accumulation of low-cap ticker PENY detected."},
                    {"account": "ACC_INSIDER", "decision": "FLAG", "violation_type": "INSIDER_TRADING", "severity": "HIGH", "reason": "Outlier volume detected in BIOX."}
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

    def _log_violation_to_db(self, report: FlaggedAccount, action_taken: str):
        """Persists AI reasoning and action taken to the VIOLATION_LOG table."""
        conn = self._get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO VIOLATION_LOG (account_number, violation_type, severity, reason, action_taken)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    report.account,
                    report.violation_type,
                    report.severity,
                    report.reason,
                    action_taken
                ))
                conn.commit()
                cur.close()
                logger.info(f"Persisted violation for {report.account} to DB.")
            except Exception as e:
                logger.error(f"Failed to log violation to DB: {e}")
            finally:
                conn.close()

    def _block_account(self, account: str, reason: str):
        """Blocks an account in Redis for 24 hours."""
        try:
            block_key = f"blocked_account:{account}"
            # Store for 24h (86400s)
            self.redis.setex(block_key, 86400, reason)
            logger.warning(f"!!! ACCOUNT BLOCKED !!! {account} | Reason: {reason}")
        except Exception as e:
            logger.error(f"Failed to block account {account} in Redis: {e}")

    def _perform_batch_ai_audit(self):
        """Performs AI audits for all accounts that traded in the last window."""
        active_provider = os.getenv("LLM_PROVIDER", "unknown")
        try:
            active_accounts = list(self.redis.smembers("active_accounts_this_window"))
            if not active_accounts: return

            logger.info(f" [GLOBAL AUDIT] Window closed. Auditing {len(active_accounts)} accounts individually...")
            
            all_flagged_accounts = []
            
            for account in active_accounts:
                try:
                    # 1. Start with Redis Cache (Real-time window)
                    history_key = f"history:{account}"
                    redis_history = self.redis.lrange(history_key, 0, -1)
                    
                    # 2. Rehydrate with PG history for the full 5-day window
                    db_history = self._get_history_from_db(account)
                    
                    # 3. Deduplicate and merge
                    merged_history = []
                    seen_ids = set()
                    for entry in db_history + redis_history:
                        if "ID:" in entry:
                            tid = entry.split("ID:")[1].strip()
                            if tid not in seen_ids:
                                merged_history.append(entry)
                                seen_ids.add(tid)
                        else:
                            merged_history.append(entry)

                    batch_data = f"ACCOUNT: {account}\n" + "\n".join([f"- {item}" for item in merged_history])
                    
                    # Execute AI call for THIS account
                    result = self.chain.invoke({"batch_data": batch_data})
                    
                    # Normalize result to dict
                    if hasattr(result, 'model_dump'):
                        flagged_list = result.model_dump().get('flagged_accounts', [])
                    else:
                        flagged_list = json.loads(result).get('flagged_accounts', [])
                    
                    if flagged_list:
                        for report_dict in flagged_list:
                            report = FlaggedAccount(**report_dict) if isinstance(report_dict, dict) else report_dict
                            
                            # Only process the report if it actually matches the account we asked about
                            if report.account != account and report.account != "SYSTEM":
                                continue

                            action = "NOTIFIED"
                            if report.decision == "BLOCK":
                                self._block_account(report.account, report.reason)
                                action = "BLOCKED"
                            elif report.decision == "FLAG":
                                action = "FLAGGED"
                            
                            if report.decision in ["FLAG", "BLOCK"]:
                                self._log_violation_to_db(report, action)
                            
                            all_flagged_accounts.append(report.model_dump() if hasattr(report, 'model_dump') else report_dict)

                except Exception as e:
                    logger.error(f"Audit failed for {account}: {e}")

            if all_flagged_accounts or active_accounts:
                notification_event = {
                    "event_type": "BATCH_PDT_ALERT",
                    "flagged_accounts": all_flagged_accounts,
                    "audited_accounts": active_accounts,
                    "total_audited_count": len(active_accounts),
                    "provider": active_provider,
                    "timestamp": datetime.now().isoformat()
                }
                actually_flagged = [a for a in all_flagged_accounts if a.get('decision') in ['FLAG', 'BLOCK']]
                if actually_flagged:
                    logger.warning(f"!!! COMPLIANCE ALERT !!! AI flagged {len(actually_flagged)} accounts.")
                
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
