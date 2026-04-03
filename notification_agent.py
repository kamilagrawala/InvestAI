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
from crypto_utils import decrypt_string

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
            accounts = [a.get('account') for r in event.get('flagged_accounts', [])]
            logger.info(f" [NOTIFICATION LOG] BATCH ALERT: Flagged {len(accounts)} accounts: {accounts}")
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
        
        msg = MIMEMultipart()
        msg['From'] = self.email_user
        msg['To'] = self.email_user
        msg['Subject'] = f"InvestAI GLOBAL SUMMARY: {len(event.get('flagged_accounts', []))} Violations Detected"

        body = "InvestAI Audit Summary Report\n"
        body += "=" * 30 + "\n\n"
        
        for account_report in event.get('flagged_accounts', []):
            body += f"ACCOUNT: {account_report.get('account')}\n"
            body += f"DECISION: {account_report.get('decision')}\n"
            body += f"REASON: {account_report.get('reason')}\n"
            body += "-" * 20 + "\n"

        body += f"\nTimestamp: {event.get('timestamp')}"
        msg.attach(MIMEText(body, 'plain'))

        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.email_user, self.email_pass)
            server.send_message(msg)
            server.quit()
            logger.info(f" [v] Global Summary Email sent successfully.")
        except Exception as e:
            logger.error(f" [x] Failed to send email: {e}")

class NotificationAgent:
    def __init__(self):
        self.channels = [LogChannel()]
        
        email_user = os.getenv("EMAIL_USER")
        email_pass_enc = os.getenv("EMAIL_PASS")
        if email_user and email_pass_enc:
            try:
                email_pass = decrypt_string(email_pass_enc, env_name="GOOGLE_MASTER_KEY")
                self.channels.append(EmailChannel(email_user, email_pass))
                logger.info("Email channel initialized.")
            except Exception as e:
                logger.error(f"Failed to initialize email channel: {e}")

    def dispatch(self, event):
        # Now we just dispatch whatever batch we get. 
        # Throttling is handled by the Audit Agent's global window.
        for channel in self.channels:
            try:
                channel.send(event)
            except Exception as e:
                logger.error(f"Failed to send notification via {channel.__class__.__name__}: {e}")

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
