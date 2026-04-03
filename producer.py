import pika
import json
from datetime import datetime

def send_trade(ticker, price, account_number, action='BUY'):
    credentials = pika.PlainCredentials('admin', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=credentials))
    channel = connection.channel()

    # Durable=True survives RabbitMQ restart
    channel.queue_declare(queue='stock_trades', durable=True)

    trade_data = {
        'Ticker': ticker,
        'Price': price,
        'Action': action,
        'Date': datetime.now().isoformat(),
        'Account Number': account_number
    }

    message = json.dumps(trade_data)
    
    # delivery_mode=2 makes the message persistent on disk
    channel.basic_publish(
        exchange='amq.direct',
        routing_key='stock_trades',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2, 
        ))
    print(f" [x] Sent {message}")
    connection.close()

if __name__ == "__main__":
    send_trade('AAPL', 150.25, 'ACC12345')
    send_trade('GOOGL', 2800.75, 'ACC67890')
    send_trade('EQNR', 42.75, 'ACC12345')
