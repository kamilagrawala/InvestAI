import pika
import json

def peek_queue(queue_name):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the queue (idempotent)
    channel.queue_declare(queue=queue_name)

    print(f"Peeking into queue: {queue_name}\n")

    while True:
        # basic_get is used to fetch a single message
        # requeue=True ensures the message stays in the queue after we read it
        method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=False)
        
        if method_frame:
            # Requeue=True means the message is put back in the queue
            channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
            
            message = json.loads(body)
            print(f"Message {method_frame.delivery_tag}:")
            print(json.dumps(message, indent=4))
            print("-" * 20)
            
            # To avoid infinite loop (since we're requeueing), we'll stop after a few or use a counter
            if method_frame.delivery_tag >= 10: # arbitrary limit for peeking
                break
        else:
            print("No more messages in queue.")
            break

    connection.close()

if __name__ == "__main__":
    peek_queue('stock_trades')
