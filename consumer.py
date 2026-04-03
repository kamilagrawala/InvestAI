import pika
import json
import logging
import time
import sys
import os
from crypto_utils import decrypt_string

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def callback(ch, method, properties, body):
    start_time = time.time()
    try:
        trade_data = json.loads(body)
        ticker = trade_data.get('Ticker')
        action = trade_data.get('Action', 'UNKNOWN')
        trade_id = trade_data.get('TradeID', 'N/A')
        
        # Log precisely when processing starts
        logger.info(f"START Processing trade {trade_id}: {action} {ticker}")
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        duration = (time.time() - start_time) * 1000 # milliseconds
        logger.info(f"END Processed trade {trade_id} in {duration:.2f}ms")
        sys.stdout.flush()
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        sys.stdout.flush()

def main():
    # Use environment variables for RabbitMQ configuration
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    rabbitmq_user_raw = os.getenv('RABBITMQ_USER', 'guest')
    rabbitmq_pass_raw = os.getenv('RABBITMQ_PASS', 'guest')
    
    # Decrypt credentials if they look like encrypted strings
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
            
    except Exception as e:
        logger.error(f"Failed to decrypt RabbitMQ credentials: {e}")
        rabbitmq_user = rabbitmq_user_raw
        rabbitmq_pass = rabbitmq_pass_raw

    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    
    while True:
        try:
            logger.info(f'Connecting to RabbitMQ at {rabbitmq_host} (user: {rabbitmq_user})...')
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=rabbitmq_host,
                port=5672,
                credentials=credentials,
                heartbeat=60
            ))
            channel = connection.channel()

            channel.queue_declare(queue='stock_trades', durable=True)
            channel.queue_bind(exchange='amq.direct', queue='stock_trades', routing_key='stock_trades')

            # Higher prefetch means it can grab more messages at once
            channel.basic_qos(prefetch_count=50)

            channel.basic_consume(
                queue='stock_trades', 
                on_message_callback=callback,
                auto_ack=False 
            )

            logger.info(' [*] SUBSCRIPTION ACTIVE. Fast processing.')
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            logger.warning("Connection lost. Retrying in 5 seconds...")
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Stopped by user.")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()
