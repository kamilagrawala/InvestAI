import pika
import json
import logging
import os
import sys
import time
import smtplib
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
        logger.info(f" [NOTIFICATION LOG] {event.get('event_type')}: "
                    f"Account {event.get('account_number')} for {event.get('ticker')}. "
                    f"Details: {event.get('details')}")

class EmailChannel(NotificationChannel):
    def __init__(self, email_user, email_pass):
        self.email_user = email_user
        self.email_pass = email_pass
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587

    def send(self, event):
        logger.info(f" [EMAIL CHANNEL] Sending email alert for {event.get('account_number')}...")
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = self.email_user
        msg['To'] = self.email_user # Sending to self for now
        msg['Subject'] = f"InvestAI ALERT: Day Trader Detected ({event.get('account_number')})"

        body = (
            f"Alert Type: {event.get('event_type')}\n"
            f"Account: {event.get('account_number')}\n"
            f"Ticker: {event.get('ticker')}\n"
            f"Trade Count: {event.get('trade_count')}\n"
            f"Analysis: {event.get('details')}\n"
            f"Timestamp: {event.get('timestamp')}"
        )
        msg.attach(MIMEText(body, 'plain'))

        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls() # Secure the connection
            server.login(self.email_user, self.email_pass)
            server.send_message(msg)
            server.quit()
            logger.info(f" [v] Email sent successfully to {self.email_user}")
        except Exception as e:
            logger.error(f" [x] Failed to send email: {e}")

class NotificationAgent:
    def __init__(self):
        self.channels = [LogChannel()]
        
        # Add email channel if credentials provided
        email_user = os.getenv("EMAIL_USER")
        email_pass_enc = os.getenv("EMAIL_PASS")
        if email_user and email_pass_enc:
            try:
                email_pass = decrypt_string(email_pass_enc, env_name="GOOGLE_MASTER_KEY")
                self.channels.append(EmailChannel(email_user, email_pass))
                logger.info("Email channel initialized with encrypted credentials.")
            except Exception as e:
                logger.error(f"Failed to initialize email channel: {e}")

    def dispatch(self, event):
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

        while True:
            try:
                credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
                connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=rabbitmq_host,
                    credentials=credentials,
                    heartbeat=60
                ))
                channel = connection.channel()

                # Ensure queue exists
                channel.queue_declare(queue='notifications', durable=True)
                channel.queue_bind(exchange='amq.direct', queue='notifications', routing_key='notifications')

                def callback(ch, method, properties, body):
                    event = json.loads(body)
                    self.dispatch(event)
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_consume(queue='notifications', on_message_callback=callback)
                
                logger.info("Notification Agent active. Waiting for events...")
                channel.start_consuming()

            except pika.exceptions.AMQPConnectionError:
                logger.warning("Connection lost. Retrying in 5 seconds...")
                time.sleep(5)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(5)

if __name__ == "__main__":
    agent = NotificationAgent()
    agent.start()
