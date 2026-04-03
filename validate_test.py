import re
import os
import subprocess
from datetime import datetime

def check_rabbitmq():
    try:
        result = subprocess.check_output(['rabbitmqctl', 'list_queues', 'name', 'messages_ready']).decode()
        for line in result.split('\n'):
            if 'stock_trades' in line:
                ready = int(line.split()[1])
                return ready
    except Exception as e:
        print(f"Error checking RabbitMQ: {e}")
    return -1

def validate():
    print("--- STARTING VALIDATION ---")
    
    # 1. Check RabbitMQ Queue status
    ready = check_rabbitmq()
    print(f"Messages remaining in RabbitMQ: {ready}")
    
    # 2. Parse producer.log for Sent Trades
    sent_trades = {}
    if os.path.exists('producer.log'):
        with open('producer.log', 'r') as f:
            for line in f:
                # Format: [2026-04-02T10:13:00.123456] [x] Sent TradeID: 0
                match = re.search(r'\[(.*?)\] \[x\] Sent TradeID: (\d+)', line)
                if match:
                    ts = match.group(1)
                    trade_id = int(match.group(2))
                    sent_trades[trade_id] = ts
    
    # 3. Parse consumer.log for Processed Trades
    processed_trades = {}
    if os.path.exists('consumer.log'):
        with open('consumer.log', 'r') as f:
            for line in f:
                # Format: 2026-04-02 10:20:45,123 - INFO - Processing trade 0 for LOAD
                match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*Processing trade (\d+)', line)
                if match:
                    timestamp_str = match.group(1)
                    trade_id = int(match.group(2))
                    processed_trades[trade_id] = timestamp_str

    print(f"Trades Sent (Producer): {len(sent_trades)}")
    print(f"Trades Processed (Consumer): {len(processed_trades)}")
    
    # 4. Compare and Match
    missing = []
    timestamp_errors = []
    for tid in range(50):
        if tid not in sent_trades:
            missing.append(f"Trade {tid} NOT SENT")
        if tid not in processed_trades:
            missing.append(f"Trade {tid} NOT PROCESSED")
        
        # Timing check
        if tid in sent_trades and tid in processed_trades:
            # Basic check: were they both found? 
            pass

    if not missing:
        print("SUCCESS: All 50 trades were SENT and PROCESSED.")
        # Sample timestamp check for Trade 0 and 49
        for tid in [0, 49]:
            print(f"Verification: Trade {tid} | Sent: {sent_trades[tid]} | Processed: {processed_trades[tid]}")
    else:
        print("FAILURE: Issues detected:")
        for m in missing[:10]: # Limit output
            print(f"  - {m}")
        if len(missing) > 10:
            print(f"  ... and {len(missing)-10} more.")

if __name__ == "__main__":
    validate()
