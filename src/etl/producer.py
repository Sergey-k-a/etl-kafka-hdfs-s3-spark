# scripts/producer.py
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='shkafka:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

events = ['page_view', 'click', 'purchase', 'login', 'logout']
users = [f'user_{i}' for i in range(1, 101)]

print("Starting producer...")
while True:
    event = {
        'event_type': random.choice(events),
        'user_id': random.choice(users),
        'timestamp': datetime.now().isoformat(),
        'value': round(random.uniform(1, 1000), 2),
        'page': f'/page/{random.randint(1, 50)}'
    }
    
    key = event['user_id']
    producer.send('raw-events', key=key, value=event)
    print(f"Sent: {event['event_type']} from {key}")
    
    time.sleep(random.uniform(0.1, 1))
