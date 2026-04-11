import json
import logging
import time
import sys
import os
import pika
import psycopg2
import redis
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

# Redis setup for block-list checking
redis_host = os.getenv("REDIS_HOST", "localhost")
r_client = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)

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
            
            # Create VIOLATION_LOG table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS VIOLATION_LOG (
                    id SERIAL PRIMARY KEY,
                    account_number VARCHAR(50),
                    violation_type VARCHAR(50),
                    severity VARCHAR(20),
                    reason TEXT,
                    action_taken VARCHAR(50),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            
            cur.close()
            logger.info("Database initialized with VIOLATION_LOG.")
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

def is_account_blocked(account_number):
    """Layered check: 1. Redis (hot), 2. DB (durable)"""
    if not account_number: return False
    
    # 1. Hot Check (Redis)
    try:
        if r_client.exists(f"blocked_account:{account_number}"):
            return True
    except Exception as e:
        logger.error(f"Redis check failed: {e}")

    # 2. Durable Check (DB)
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM VIOLATION_LOG WHERE account_number = %s AND action_taken = 'BLOCKED' LIMIT 1", (account_number,))
            blocked = cur.fetchone() is not None
            cur.close()
            
            # 3. Optional: Back-fill Redis if found in DB for next time
            if blocked:
                try:
                    r_client.setex(f"blocked_account:{account_number}", 3600, "Primed from DB")
                except: pass
                
            return blocked
        except Exception as e:
            logger.error(f"DB block check failed: {e}")
        finally:
            conn.close()
    return False

def callback(ch, method, properties, body):
    start_time = time.time()
    try:
        trade_data = json.loads(body)
        ticker = trade_data.get('Ticker')
        action = trade_data.get('Action', 'UNKNOWN')
        trade_id = trade_data.get('TradeID', 'N/A')
        account = trade_data.get('Account Number')
        
        logger.info(f"START Processing trade {trade_id}: {action} {ticker}")
        
        # GATEKEEPER: Check if account is blocked
        if is_account_blocked(account):
            print(f"\n[GATEKEEPER] BLOCKING TRADE: {trade_id} from {account}\n", flush=True)
            logger.warning(f"!!! DROPPED_TRADE !!! Rejected trade {trade_id} from blocked account: {account}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

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
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    
    # DB initialization
    init_db()

    # RabbitMQ connection with retries
    max_retries = 10
    for attempt in range(max_retries):
        try:
            rabbitmq_user_raw = os.getenv('RABBITMQ_USER', 'admin')
            rabbitmq_pass_raw = os.getenv('RABBITMQ_PASS', 'password')
            
            r_user = decrypt_string(rabbitmq_user_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_user_raw) > 50 else rabbitmq_user_raw
            r_pass = decrypt_string(rabbitmq_pass_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_pass_raw) > 50 else rabbitmq_pass_raw

            credentials = pika.PlainCredentials(r_user, r_pass)
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
            channel = connection.channel()

            channel.queue_declare(queue='stock_trades', durable=True)
            channel.basic_qos(prefetch_count=50)
            channel.basic_consume(queue='stock_trades', on_message_callback=callback)

            logger.info(" [*] SUBSCRIPTION ACTIVE. Gatekeeper enabled.")
            channel.start_consuming()
            break
        except pika.exceptions.AMQPConnectionError:
            logger.warning(f"RabbitMQ connection attempt {attempt+1} failed. Retrying...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()
