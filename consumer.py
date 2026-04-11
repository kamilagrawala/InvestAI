import json
import logging
import time
import sys
import os
import pika
import psycopg2
from dotenv import load_dotenv
from crypto_utils import decrypt_string

# Force load .env
load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    try:
        db_host = os.getenv('DB_HOST', 'localhost')
        db_name = os.getenv('DB_NAME', 'investai')
        
        # Decrypt DB credentials
        db_user_enc = os.getenv('DB_USER_ENCRYPTED')
        db_pass_enc = os.getenv('DB_PASS_ENCRYPTED')
        
        db_user = decrypt_string(db_user_enc, env_name="POSTGRES_MASTER_KEY")
        db_pass = decrypt_string(db_pass_enc, env_name="POSTGRES_MASTER_KEY")
        
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_pass
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            # Rename if old table exists
            cur.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'trades');")
            if cur.fetchone()[0]:
                logger.info("Renaming existing 'trades' table to 'TRADEORDER'.")
                cur.execute("ALTER TABLE trades RENAME TO TRADEORDER;")
                conn.commit()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS TRADEORDER (
                    id SERIAL PRIMARY KEY,
                    trade_id VARCHAR(50) UNIQUE,
                    account_number VARCHAR(50),
                    ticker VARCHAR(10),
                    action VARCHAR(10),
                    price DECIMAL,
                    trade_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            cur.close()
            logger.info("Database initialized.")
        finally:
            conn.close()

def save_trade(trade_data):
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO TRADEORDER (trade_id, account_number, ticker, action, price, trade_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_id) DO NOTHING
            """, (
                trade_data.get('TradeID'),
                trade_data.get('Account Number'),
                trade_data.get('Ticker'),
                trade_data.get('Action'),
                trade_data.get('Price'),
                trade_data.get('Date')
            ))
            conn.commit()
            cur.close()
        except Exception as e:
            logger.error(f"Failed to save trade to DB: {e}")
        finally:
            conn.close()

def callback(ch, method, properties, body):
    start_time = time.time()
    try:
        trade_data = json.loads(body)
        ticker = trade_data.get('Ticker')
        action = trade_data.get('Action', 'UNKNOWN')
        trade_id = trade_data.get('TradeID', 'N/A')
        
        logger.info(f"START Processing trade {trade_id}: {action} {ticker}")
        
        # 1. PERSIST TO POSTGRES
        save_trade(trade_data)
        
        # 2. Acknowledge
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        duration = (time.time() - start_time) * 1000
        logger.info(f"END Processed trade {trade_id} in {duration:.2f}ms")
        sys.stdout.flush()
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        sys.stdout.flush()

def main():
    # DB initialization with more aggressive retries for legacy containers
    max_db_retries = 20
    for i in range(max_db_retries):
        try:
            init_db()
            break
        except Exception as e:
            logger.warning(f"Database not ready (attempt {i+1}/{max_db_retries})... Error: {e}")
            time.sleep(10)

    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    rabbitmq_user_raw = os.getenv('RABBITMQ_USER', 'guest')
    rabbitmq_pass_raw = os.getenv('RABBITMQ_PASS', 'guest')
    
    try:
        rabbitmq_user = decrypt_string(rabbitmq_user_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_user_raw) > 50 else rabbitmq_user_raw
        rabbitmq_pass = decrypt_string(rabbitmq_pass_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_pass_raw) > 50 else rabbitmq_pass_raw
    except Exception as e:
        logger.error(f"Decryption failed: {e}")
        rabbitmq_user, rabbitmq_pass = rabbitmq_user_raw, rabbitmq_pass_raw

    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=rabbitmq_host, port=5672, credentials=credentials, heartbeat=60
            ))
            channel = connection.channel()
            channel.queue_declare(queue='stock_trades', durable=True)
            channel.queue_bind(exchange='amq.direct', queue='stock_trades', routing_key='stock_trades')
            channel.basic_qos(prefetch_count=50)
            channel.basic_consume(queue='stock_trades', on_message_callback=callback, auto_ack=False)
            logger.info(' [*] SUBSCRIPTION ACTIVE. Storage enabled.')
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            logger.warning("Connection lost. Retrying...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()
