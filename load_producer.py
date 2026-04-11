import pika
import json
import os
import random
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
from crypto_utils import decrypt_string

# Force load .env to override OS variables
load_dotenv(override=True)

def send_trades():
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
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
            break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"RabbitMQ not ready (attempt {attempt+1}/{max_retries}). Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(f"Error: Could not connect to RabbitMQ after {max_retries} attempts.")
                return

    try:
        channel.queue_declare(queue='stock_trades', durable=True)
        channel.queue_bind(exchange='amq.direct', queue='stock_trades', routing_key='stock_trades')

        trades_plan = []
        
        # 1. ACC_0 & ACC_1: 4 Round Trips each -> Guaranteed FLAG (PDT)
        for acc in ['ACC_0', 'ACC_1']:
            for i in range(4):
                trades_plan.append({'account': acc, 'action': 'BUY', 'ticker': 'NVDA'})
                trades_plan.append({'account': acc, 'action': 'SELL', 'ticker': 'NVDA'})
        
        # 2. ACC_SPOOF: 10 Rapid BUYs at increasing prices, then a single large SELL.
        for i in range(10):
            trades_plan.append({'account': 'ACC_SPOOF', 'action': 'BUY', 'ticker': 'AAPL', 'price_offset': i * 0.1})
        trades_plan.append({'account': 'ACC_SPOOF', 'action': 'SELL', 'ticker': 'AAPL', 'price_offset': 1.0})

        # 3. ACC_PUMP: Rapid accumulation of a low-cap stock.
        for i in range(8):
            trades_plan.append({'account': 'ACC_PUMP', 'action': 'BUY', 'ticker': 'PENY'})
        
        # 4. ACC_INSIDER: A single massive BUY trade right before a "Market News" event.
        trades_plan.append({'account': 'ACC_INSIDER', 'action': 'BUY', 'ticker': 'BIOX', 'price': 500.0})

        # 5. ACC_4: Regular activity
        trades_plan.append({'account': 'ACC_4', 'action': 'BUY', 'ticker': 'GOOGL'})
        trades_plan.append({'account': 'ACC_4', 'action': 'SELL', 'ticker': 'GOOGL'})

        full_audit_log = []
        print(f"[{datetime.now().isoformat()}] Sending {len(trades_plan)} compliance test trades...")
        
        for i, trade in enumerate(trades_plan):
            base_price = trade.get('price', 100.0)
            offset = trade.get('price_offset', 0.0)
            
            trade_data = {
                'Ticker': trade['ticker'],
                'Price': base_price + offset + i,
                'Action': trade['action'],
                'Date': datetime.now().isoformat(),
                'Account Number': trade['account'],
                'TradeID': f"SEC-TEST-{i}"
            }
            
            # Save ALL fields for the independent audit
            full_audit_log.append(trade_data)
            
            channel.basic_publish(
                exchange='amq.direct',
                routing_key='stock_trades',
                body=json.dumps(trade_data),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"Sent: {trade['account']} | {trade['action']} | {trade['ticker']}")

        # Save the full history to a file
        with open('audit_history.json', 'w') as f:
            json.dump(full_audit_log, f, indent=2)

        connection.close()
        print(f"Finished. Full history saved to audit_history.json")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    send_trades()
