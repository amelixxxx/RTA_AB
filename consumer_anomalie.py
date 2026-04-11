from kafka import KafkaConsumer
from collections import defaultdict
import json
import time


consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

user_transaction_times = {}

for message in consumer:
    tx = message.value
    user_id = tx.get('user_id')
    current_time = time.time()

    if user_id not in user_transaction_times:
        user_transaction_times[user_id] = []
    
    user_transaction_times[user_id].append(current_time)
    
    active_window = []
    for t in user_transaction_times[user_id]:
        if current_time - t <= 60:
            active_window.append(t)
            
    user_transaction_times[user_id] = active_window
    
    if len(user_transaction_times[user_id]) > 3:
        print(f"ALERT: User {user_id} przekroczył limit prędkości")
