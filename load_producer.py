import pika
import json
from datetime import datetime
import sys

def send_trades_on_connection(count=50):
    try:
        credentials = pika.PlainCredentials('admin', 'password')
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
        channel = connection.channel()

        channel.queue_declare(queue='stock_trades', durable=True)
        channel.queue_bind(exchange='amq.direct', queue='stock_trades', routing_key='stock_trades')

        print(f"[{datetime.now().isoformat()}] Starting to send {count} trades...")
        import random
        for i in range(count):
            action = random.choice(['BUY', 'SELL'])
            trade_data = {
                'Ticker': 'LOAD',
                'Price': 100.0 + (i % 100),
                'Action': action,
                'Date': datetime.now().isoformat(),
                'Account Number': f'ACC_{i % 5}', # Reusing accounts to trigger day trader logic
                'TradeID': i
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
            
            # Print every 10 for smaller batches, or every 1000 for large ones
            if count <= 100 or i % 1000 == 0 or i == count - 1:
                print(f"[{send_ts}] [x] Sent TradeID: {i}")
                sys.stdout.flush()
        
        connection.close()
        print(f"[{datetime.now().isoformat()}] Finished sending {count} trades.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Allow count to be passed as an argument
    trade_count = 50
    if len(sys.argv) > 1:
        try:
            trade_count = int(sys.argv[1])
        except ValueError:
            pass
    send_trades_on_connection(trade_count)
