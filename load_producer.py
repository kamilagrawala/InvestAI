import pika
import json
from datetime import datetime
import sys
import os
from crypto_utils import decrypt_string

def send_trades_on_connection(count=50, instance_id=1):
    try:
        # ... existing decryption and connection setup ...
        rabbitmq_user_raw = os.getenv('RABBITMQ_USER', 'admin')
        rabbitmq_pass_raw = os.getenv('RABBITMQ_PASS', 'password')
        
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
        except Exception:
            rabbitmq_user = rabbitmq_user_raw
            rabbitmq_pass = rabbitmq_pass_raw

        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
        channel = connection.channel()

        channel.queue_declare(queue='stock_trades', durable=True)
        channel.queue_bind(exchange='amq.direct', queue='stock_trades', routing_key='stock_trades')

        import random
        print(f"[{datetime.now().isoformat()}] [Producer-{instance_id}] Starting to send {count} trades...")
        
        for i in range(count):
            account_idx = i % 5
            account = f'ACC_{account_idx}'
            
            # Precise constraint: Max 2 trades total for ACC_3 and ACC_4 per producer
            if account_idx == 3 and i >= 15: # ACC_3 appears at 3, 8, 13... so 15 stops it after 3 trades
                continue
            if account_idx == 4 and i >= 15:
                continue
            
            # To get exactly 2 trades for 3 and 4 as requested:
            if account_idx >= 3 and i >= 10:
                continue

            action = random.choice(['BUY', 'SELL'])
            trade_data = {
                'Ticker': 'LOAD',
                'Price': 100.0 + (i % 100),
                'Action': action,
                'Date': datetime.now().isoformat(),
                'Account Number': account,
                'TradeID': f"P{instance_id}-{i}" # Unique Trade ID per producer
            }

            message = json.dumps(trade_data)
            send_ts = datetime.now().isoformat()
            
            channel.basic_publish(
                exchange='amq.direct',
                routing_key='stock_trades',
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2, 
                ))
            
            if count <= 100 or i % 1000 == 0:
                print(f"[{send_ts}] [Producer-{instance_id}] [x] Sent TradeID: {trade_data['TradeID']}")
                sys.stdout.flush()
        
        connection.close()
        print(f"[{datetime.now().isoformat()}] [Producer-{instance_id}] Finished.")
    except Exception as e:
        print(f"Error in Producer-{instance_id}: {e}")

if __name__ == "__main__":
    # Usage: python load_producer.py <count> <instance_id>
    trade_count = 50
    p_id = 1
    if len(sys.argv) > 1:
        trade_count = int(sys.argv[1])
    if len(sys.argv) > 2:
        p_id = int(sys.argv[2])
        
    send_trades_on_connection(trade_count, p_id)
