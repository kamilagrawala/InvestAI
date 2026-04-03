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

        # Start the background Watchdog to send final updates
        self.watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)
        self.watchdog_thread.start()

    def _watchdog_loop(self):
        """Background thread that checks for accounts needing a final summary email."""
        logger.info("Watchdog background thread started.")
        while True:
            try:
                # Find all accounts that were marked as 'pending'
                pending_accounts = self.redis.smembers("pending_notifications")
                
                for account in pending_accounts:
                    throttle_key = f"email_throttle:{account}"
                    # If the throttle has expired, we can send the final update
                    if not self.redis.get(throttle_key):
                        logger.info(f" [WATCHDOG] Cooldown expired for {account}. Sending final summary...")
                        
                        # Retrieve last stored event data from Redis
                        event_data_json = self.redis.get(f"pending_event_data:{account}")
                        if event_data_json:
                            event = json.loads(event_data_json)
                            # Actual dispatch
                            for channel in self.channels:
                                try:
                                    channel.send(event)
                                except Exception as e:
                                    logger.error(f"Failed to send notification via {channel.__class__.__name__}: {e}")
                            
                            # Update Redis state
                            self.redis.setex(throttle_key, self.throttle_seconds, "active")
                            self.redis.set(f"last_reported_count:{account}", event.get('trade_count'))
                        
                        # Remove from pending set
                        self.redis.srem("pending_notifications", account)
                
            except Exception as e:
                logger.error(f"Watchdog error: {e}")
            
            time.sleep(5) # Check every 5 seconds

    def dispatch(self, event):
        # Throttling logic for notifications
        account = event.get('account_number')
        current_count = event.get('trade_count', 0)
        
        # Keys for Redis
        throttle_key = f"email_throttle:{account}"
        last_count_key = f"last_reported_count:{account}"
        pending_set_key = "pending_notifications"
        pending_data_key = f"pending_event_data:{account}"
        
        is_throttled = self.redis.get(throttle_key)
        last_reported_count = self.redis.get(last_count_key)
        
        if is_throttled:
            # Mark as pending so Watchdog can send the final update later
            self.redis.sadd(pending_set_key, account)
            self.redis.set(pending_data_key, json.dumps(event))
            logger.info(f" [THROTTLED] Event for {account} marked as PENDING (Final count will be {current_count}).")
            return

        # If 60s has passed, only send if the count has changed
        if last_reported_count and int(last_reported_count) == current_count:
            # Check if it was pending (unlikely here but for safety)
            self.redis.srem(pending_set_key, account)
            logger.info(f" [SKIP] No change in trade count ({current_count}) for {account}. Notification suppressed.")
            return

        # Send through all active channels (Log, Email, etc.)
        for channel in self.channels:
            try:
                channel.send(event)
            except Exception as e:
                logger.error(f"Failed to send notification via {channel.__class__.__name__}: {e}")
        
        # Update Redis: Set throttle window and update last reported count
        self.redis.setex(throttle_key, self.throttle_seconds, "active")
        self.redis.set(last_count_key, current_count)
        # Ensure it's removed from pending since we just sent it
        self.redis.srem(pending_set_key, account)

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
