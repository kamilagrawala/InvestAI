import pika
import json
import os
import random
import sys
from datetime import datetime
from crypto_utils import decrypt_string

def send_trades():
    try:
        rabbitmq_user_raw = os.getenv('RABBITMQ_USER', 'admin')
        rabbitmq_pass_raw = os.getenv('RABBITMQ_PASS', 'password')
        
        try:
            r_user = decrypt_string(rabbitmq_user_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_user_raw) > 50 else rabbitmq_user_raw
            r_pass = decrypt_string(rabbitmq_pass_raw, env_name="RABBITMQ_MASTER_KEY") if len(rabbitmq_pass_raw) > 50 else rabbitmq_pass_raw
        except Exception:
            r_user, r_pass = rabbitmq_user_raw, rabbitmq_pass_raw

        credentials = pika.PlainCredentials(r_user, r_pass)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=credentials))
        channel = connection.channel()

        channel.queue_declare(queue='stock_trades', durable=True)
        channel.queue_bind(exchange='amq.direct', queue='stock_trades', routing_key='stock_trades')

        trades_plan = []
        
        # 1. ACC_0 & ACC_1: 4 Round Trips each -> Guaranteed FLAG
        for acc in ['ACC_0', 'ACC_1']:
            for i in range(4):
                trades_plan.append({'account': acc, 'action': 'BUY', 'ticker': 'NVDA'})
                trades_plan.append({'account': acc, 'action': 'SELL', 'ticker': 'NVDA'})
        
        # 2. ACC_2 & ACC_3: 1 Trade each -> Guaranteed PASS
        for acc in ['ACC_2', 'ACC_3']:
            trades_plan.append({'account': acc, 'action': 'BUY', 'ticker': 'AAPL'})
            
        # 3. ACC_4: EXACTLY 3 Trades (BUY, BUY, SELL) -> 1 Round Trip. MUST BE A PASS.
        trades_plan.append({'account': 'ACC_4', 'action': 'BUY', 'ticker': 'GOOGL'})
        trades_plan.append({'account': 'ACC_4', 'action': 'BUY', 'ticker': 'GOOGL'})
        trades_plan.append({'account': 'ACC_4', 'action': 'SELL', 'ticker': 'GOOGL'})

        full_audit_log = []
        print(f"[{datetime.now().isoformat()}] Sending 21 deterministic trades...")
        
        for i, trade in enumerate(trades_plan):
            trade_data = {
                'Ticker': trade['ticker'],
                'Price': 100.0 + i,
                'Action': trade['action'],
                'Date': datetime.now().isoformat(),
                'Account Number': trade['account'],
                'TradeID': f"FINAL-{i}"
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
