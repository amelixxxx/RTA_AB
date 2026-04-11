from kafka import KafkaConsumer
from collections import defaultdict
import json

# TWÓJ KOD
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

counts = defaultdict(int)
totals = defaultdict(float)
mins = {}
maxs = {}

msg_count = 0

for message in consumer:
    tx = message.value
    
    category = tx['category']
    amount = float(tx['amount'])
    
    counts[category] += 1
    totals[category] += amount
    
    if category not in mins or amount < mins[category]:
        mins[category] = amount
    
    if category not in maxs or amount > maxs[category]:
        maxs[category] = amount
    
    msg_count += 1

    if msg_count % 10 == 0:
        print("Kategoria | Liczba | Suma | Min | Max")
        
        for cat in counts:
            print(f"{cat} | {counts[cat]} | {totals[cat]} | {mins[cat]} | {maxs[cat]}")
