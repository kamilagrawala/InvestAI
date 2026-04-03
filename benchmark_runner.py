import subprocess
import time
import re
import os
from datetime import datetime

def run_command(cmd):
    return subprocess.check_output(cmd, shell=True).decode()

def parse_logs():
    # Parse Producer Timestamps
    producer_ts = []
    if os.path.exists('producer.log'):
        with open('producer.log', 'r') as f:
            for line in f:
                match = re.search(r'\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)\]', line)
                if match:
                    producer_ts.append(datetime.strptime(match.group(1), '%Y-%m-%dT%H:%M:%S.%f'))
    
    # Parse Consumer Timestamps
    consumer_ts = []
    if os.path.exists('consumer.log'):
        with open('consumer.log', 'r') as f:
            for line in f:
                match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})', line)
                if match and "Processing trade" in line:
                    consumer_ts.append(datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S,%f'))
    
    return producer_ts, consumer_ts

def benchmark():
    results = []
    
    for i in range(1, 6):
        print(f"Running iteration {i}/5...")
        
        # 1. Cleanup logs and ensure consumer is fresh
        run_command("pkill -9 -f consumer.py || true")
        if os.path.exists('producer.log'): os.remove('producer.log')
        if os.path.exists('consumer.log'): os.remove('consumer.log')
        
        run_command("nohup /Users/kamilagrawala/PycharmProjects/InvestAI/.venv/bin/python consumer.py > consumer.log 2>&1 &")
        time.sleep(2) # Give consumer time to start
        
        # 2. Run Producer
        start_wall = time.time()
        run_command("/Users/kamilagrawala/PycharmProjects/InvestAI/.venv/bin/python load_producer.py > producer.log 2>&1")
        
        # 3. Wait for consumer to finish (check rabbitmq queue)
        while True:
            out = run_command("rabbitmqctl list_queues name messages_ready")
            if "stock_trades\t0" in out or "stock_trades    0" in out:
                break
            time.sleep(0.5)
        
        time.sleep(1) # Final flush
        
        # 4. Analyze logs
        p_ts, c_ts = parse_logs()
        
        if p_ts and c_ts:
            prod_dur = (max(p_ts) - min(p_ts)).total_seconds()
            cons_dur = (max(c_ts) - min(c_ts)).total_seconds()
            total_dur = (max(c_ts) - min(p_ts)).total_seconds()
            
            results.append({
                'run': i,
                'prod': f"{prod_dur:.3f}s",
                'cons': f"{cons_dur:.3f}s",
                'total': f"{total_dur:.3f}s"
            })
        else:
            results.append({'run': i, 'prod': 'Error', 'cons': 'Error', 'total': 'Error'})

    # Print Table
    print("\n| Run | Producer Time | Consumer Time | End-to-End Time |")
    print("|-----|---------------|---------------|-----------------|")
    for r in results:
        print(f"| {r['run']}   | {r['prod']}        | {r['cons']}        | {r['total']}          |")

if __name__ == "__main__":
    benchmark()
