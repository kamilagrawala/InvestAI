import pika
import json
import logging
import os
import sys
import time
import smtplib
import redis
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from crypto_utils import decrypt_string

# Force load .env
load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("NotificationAgent")

class NotificationChannel:
    def send(self, event):
        pass

class LogChannel(NotificationChannel):
    def send(self, event):
        if event.get('event_type') == "BATCH_PDT_ALERT":
            # Only count accounts with DECISION: FLAG
            flagged = [r.get('account') for r in event.get('flagged_accounts', []) if r.get('decision') == 'FLAG']
            logger.info(f" [NOTIFICATION LOG] BATCH ALERT: Found {len(flagged)} violations: {flagged}")
        else:
            logger.info(f" [NOTIFICATION LOG] {event.get('event_type')}: Account {event.get('account_number')}")

class EmailChannel(NotificationChannel):
    def __init__(self, email_user, email_pass):
        self.email_user = email_user
        self.email_pass = email_pass
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587

    def send(self, event):
        logger.info(f" [EMAIL CHANNEL] Preparing summary email...")
        
        provider = event.get('provider', 'unknown').upper()
        status_tag = "[REAL AI]" if provider == "GEMINI" else "[FAKE/TEST]"
        
        all_reports = event.get('flagged_accounts', [])
        flagged_accounts = [a for a in all_reports if a.get('decision') in ['FLAG', 'BLOCK']]
        
        # Priority: Use the explicit count if provided, else fallback to report length
        total_count = event.get('total_audited_count', len(all_reports))
        passed_count = total_count - len(flagged_accounts)
        fail_rate = (len(flagged_accounts) / total_count * 100) if total_count > 0 else 0
        
        msg = MIMEMultipart()
        msg['From'] = self.email_user
        msg['To'] = self.email_user
        msg['Subject'] = f"{status_tag} InvestAI Audit: {len(flagged_accounts)} Compliance Violations"

        body = "InvestAI Compliance Audit Report\n"
        body += "=" * 30 + "\n\n"
        
        if not flagged_accounts:
            body += "No violations detected in this window.\n"
        else:
            for account_report in flagged_accounts:
                decision = account_report.get('decision', 'FLAG')
                body += f"ACCOUNT: {account_report.get('account')} [{decision}]\n"
                body += f"TYPE: {account_report.get('violation_type', 'N/A')}\n"
                body += f"REASON: {account_report.get('reason')}\n"
                body += "-" * 20 + "\n"

        body += f"\nSUMMARY STATISTICS:\n"
        body += f"Total Accounts Audited: {total_count}\n"
        body += f"Violations Found: {len(flagged_accounts)}\n"
        body += f"Fail Rate: {fail_rate:.1f}%\n"
        body += f"\nTimestamp: {event.get('timestamp')}"
        
        msg.attach(MIMEText(body, 'plain'))

        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.email_user, self.email_pass)
            server.send_message(msg)
            server.quit()
            logger.info(f" [v] Global Summary Email sent successfully.")
            return True
        except Exception as e:
            logger.error(f" [x] Failed to send email: {e}")
            return False

class NotificationAgent:
    def __init__(self):
        self.channels = [LogChannel()]
        
        # Redis setup for purging
        redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)

        email_user = os.getenv("EMAIL_USER")
        email_pass_enc = os.getenv("EMAIL_PASS")
        if email_user and email_pass_enc:
            try:
                email_pass = decrypt_string(email_pass_enc, env_name="GOOGLE_MASTER_KEY")
                self.channels.append(EmailChannel(email_user, email_pass))
                logger.info("Email channel initialized.")
            except Exception as e:
                logger.error(f"Failed to initialize email channel: {e}")

    def _purge_cache(self, event):
        """Purges history from Redis for accounts that were audited."""
        accounts = event.get('audited_accounts', [])
        for account in accounts:
            history_key = f"history:{account}"
            self.redis.delete(history_key)
        logger.info(f" [CACHE] Purged Redis history for {len(accounts)} audited accounts.")

    def dispatch(self, event):
        # [DEBUG]
        received_provider = event.get('provider', 'unknown')
        print(f"\n[DEBUG NOTIFY] RECEIVED EVENT FROM PROVIDER: {received_provider}\n", flush=True)

        # Track email success specifically as per requirements
        email_status = None # None means no email channel, True/False means success/failure
        
        for channel in self.channels:
            try:
                res = channel.send(event)
                if isinstance(channel, EmailChannel):
                    email_status = res
            except Exception as e:
                logger.error(f"Failed to send notification via {channel.__class__.__name__}: {e}")

        # Requirement: Purge only if email was successfully sent
        if email_status is True:
            self._purge_cache(event)
        elif email_status is False:
            logger.warning(" [CACHE] Email failed, keeping history in Redis.")
        elif email_status is None:
            # If no email channel was successfully initialized, we do not purge.
            # This ensures data is kept until an email can be sent.
            logger.info(" [CACHE] No active email channel, keeping history in Redis.")

    def start(self):
        rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        rabbitmq_user_raw = os.getenv('RABBITMQ_USER', 'admin')
        rabbitmq_pass_raw = os.getenv('RABBITMQ_PASS', 'password')
        
        try:
            r_user = decrypt_string(rabbitmq_user_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_user_raw) > 50 else rabbitmq_user_raw
            r_pass = decrypt_string(rabbitmq_pass_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_pass_raw) > 50 else rabbitmq_pass_raw
        except Exception:
            r_user, r_pass = rabbitmq_user_raw, rabbitmq_pass_raw

        while True:
            try:
                credentials = pika.PlainCredentials(r_user, r_pass)
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials, heartbeat=60))
                channel = connection.channel()

                channel.queue_declare(queue='notifications', durable=True)
                channel.queue_bind(exchange='amq.direct', queue='notifications', routing_key='notifications')

                def callback(ch, method, properties, body):
                    event = json.loads(body)
                    self.dispatch(event)
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_consume(queue='notifications', on_message_callback=callback)
                logger.info("Notification Agent active. Waiting for batch events...")
                channel.start_consuming()

            except pika.exceptions.AMQPConnectionError:
                time.sleep(5)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(5)

if __name__ == "__main__":
    agent = NotificationAgent()
    agent.start()
