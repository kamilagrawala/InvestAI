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
        
        # Redis setup for throttling
        redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
        self.throttle_seconds = 60

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

        # Enable Keyspace Notifications in Redis (if not already enabled)
        # 'Ex' means notify on Expired events
        try:
            self.redis.config_set('notify-keyspace-events', 'Ex')
            logger.info("Redis keyspace notifications enabled (Expired events).")
        except Exception as e:
            logger.warning(f"Could not enable Redis keyspace notifications: {e}")

        # Start the background Expiration Listener (Event-Based)
        self.listener_thread = threading.Thread(target=self._expiration_listener_loop, daemon=True)
        self.listener_thread.start()

    def _expiration_listener_loop(self):
        """Event-based listener that reacts the moment a throttle key expires in Redis."""
        pubsub = self.redis.pubsub()
        # Subscribe to the 'expired' event channel for DB 0
        pubsub.psubscribe("__keyevent@0__:expired")
        
        logger.info("Expiration listener thread started. Waiting for Redis events...")
        
        for message in pubsub.listen():
            if message['type'] == 'pmessage':
                expired_key = message['data']
                
                # Check if it's one of our throttle keys
                if expired_key.startswith("email_throttle:"):
                    account = expired_key.replace("email_throttle:", "")
                    self._handle_throttle_expiry(account)

    def _handle_throttle_expiry(self, account):
        """Triggered when a 60s accumulation window closes."""
        try:
            pending_data_key = f"pending_event_data:{account}"
            last_count_key = f"last_reported_count:{account}"
            event_data_json = self.redis.get(pending_data_key)

            if event_data_json:
                event = json.loads(event_data_json)
                current_count = event.get('trade_count')
                last_count = self.redis.get(last_count_key)

                # Only send if the count has changed since our last email
                if not last_count or int(last_count) != current_count:
                    logger.info(f" [HEARTBEAT] Window closed for {account}. Sending summary (Count: {current_count})...")
                    for channel in self.channels:
                        try:
                            channel.send(event)
                        except Exception as e:
                            logger.error(f"Failed to send summary notification: {e}")

                    # Update the record of what we last sent
                    self.redis.set(last_count_key, current_count)

                    # IMPORTANT: If activity is continuous, we could start a new window here.
                    # But per your requirement "send out the final count no matter what", 
                    # the next incoming trade will simply restart the window if needed.
                else:
                    logger.info(f" [SKIP] Window closed for {account} but count {current_count} already reported.")

                # Cleanup buffer
                self.redis.delete(pending_data_key)

        except Exception as e:
            logger.error(f"Error handling heartbeat expiry for {account}: {e}")


    def dispatch(self, event):
        # Heartbeat logic for notifications
        account = event.get('account_number')
        current_count = event.get('trade_count', 0)
        
        # Keys for Redis
        throttle_key = f"email_throttle:{account}"
        pending_data_key = f"pending_event_data:{account}"
        last_reported_count_key = f"last_reported_count:{account}"
        
        # 1. Update the latest data for this account (Buffer)
        self.redis.set(pending_data_key, json.dumps(event))
        
        # 2. Check if a heartbeat window is already active
        if not self.redis.get(throttle_key):
            # If no window active, start one now
            # This timer will trigger the email when it expires
            self.redis.setex(throttle_key, self.throttle_seconds, "active")
            logger.info(f" [HEARTBEAT] Started 60s accumulation window for {account}.")
        else:
            # Window is already running, just update the buffer
            logger.info(f" [BUFFERED] Updated latest count to {current_count} for {account}.")

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
