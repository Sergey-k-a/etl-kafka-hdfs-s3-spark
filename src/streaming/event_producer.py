#!/usr/bin/env python3
"""
Kafka Producer: Генератор потоковых событий
"""

import sys
sys.path.append('/app')

from confluent_kafka import Producer
from generators.data_generator import ECommerceDataGenerator
import json
import time
import random
import uuid
from datetime import datetime, timedelta

class ECommerceEventProducer:
    """Генератор и отправщик событий в Kafka"""
    
    def __init__(self, bootstrap_servers='shkafka:29092'):
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'ecommerce-producer',
            'acks': '1',                                            # Ждём подтверждение от лидера партиции
            'compression.type': 'snappy',
            'linger.ms': 5,
            'batch.size': 16384
        }
        self.producer = Producer(self.conf)
        self.generator = ECommerceDataGenerator(seed=42)
        self.products_df = None
        self.users_df = None
        
        # Топики Kafka
        self.topics = {
            'user_events': 'ecommerce.user.events',
            'orders': 'ecommerce.orders',
            'page_views': 'ecommerce.page.views',
            'user_actions': 'ecommerce.user.actions'
        }

        # В начале класса добавьте:
        self.start_date = datetime(2024, 1, 1)
        self.end_date = datetime(2024, 12, 31)
    
    def delivery_report(self, err, msg):
        """Callback для подтверждения доставки"""
        if err is not None:
            print(f'❌ Message delivery failed: {err}')
        else:
            print(f'✓ Delivered: {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')
    
    def generate_base_data(self):
        """Генерация справочных данных (один раз)"""
        print("📦 Generating reference data...")
        self.products_df = self.generator.generate_products(num_products=500)
        self.users_df = self.generator.generate_users(num_users=5000)
        print(f"   ✓ {len(self.products_df)} products, {len(self.users_df)} users")
    
    def produce_event(self, topic, key, value):
        """Отправка события в Kafka"""
        try:
            self.producer.produce(
                topic=topic,
                key=str(key),
                value=json.dumps(value, default=str),
                callback=self.delivery_report
            )
            self.producer.poll(0)  # Неблокирующий poll
        except BufferError:
            print(f'⚠️ Buffer full, waiting...')
            self.producer.poll(1)
            self.producer.produce(
                topic=topic,
                key=str(key),
                value=json.dumps(value, default=str)
            )
    
    def generate_user_event(self):
        """Генерация одного пользовательского события"""
        user = self.users_df.sample(1).iloc[0]
        product = self.products_df.sample(1).iloc[0] if random.random() > 0.3 else None
        
        actions = ['page_view', 'product_view', 'add_to_cart', 'search', 'click']
        action = random.choice(actions)

        random_seconds = random.randint(0, int((self.end_date - self.start_date).total_seconds()))
        random_timestamp = self.start_date + timedelta(seconds=random_seconds)
        
        event = {
            'event_id': str(uuid.uuid4()),
            'session_id': f"SES_{uuid.uuid4().hex[:12]}",
            'user_id': user['user_id'],
            'username': user['username'],
            #'event_timestamp': datetime.now().isoformat(),
            'event_timestamp': random_timestamp.isoformat(),
            'event_type': 'user_event',
            'action': action,
            'product_id': product['product_id'] if product is not None else None,
            'product_name': product['product_name'] if product is not None else None,
            'category': product['category'] if product is not None else None,
            'price': float(product['price']) if product is not None else None,
            'page_url': f"/{random.choice(['home', 'products', 'cart', 'checkout', 'profile'])}",
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
            'ip_address': self.generator.fake.ipv4(),
            'country': user['country'],
            'membership_level': user['membership_level'],
            'session_duration_sec': random.randint(1, 3600)
        }
        
        return event
    
    def generate_order_event(self):
        """Генерация события заказа"""
        user = self.users_df.sample(1).iloc[0]
        num_items = random.randint(1, 5)
        order_items = self.products_df.sample(num_items)
        
        order = {
            'order_id': f"ORD_{uuid.uuid4().hex[:8]}",
            'user_id': user['user_id'],
            'username': user['username'],
            'session_id': f"SES_{uuid.uuid4().hex[:12]}",
            'order_timestamp': datetime.now().isoformat(),
            'event_type': 'order',
            'items_count': num_items,
            'total_amount': round(order_items['price'].sum(), 2),
            'discount_applied': round(random.uniform(0, 0.3), 2),
            'shipping_cost': round(random.uniform(0, 20), 2),
            'payment_method': random.choice(['credit_card', 'paypal', 'crypto', 'bank_transfer']),
            'order_status': random.choice(['completed', 'processing', 'pending']),
            'country': user['country'],
            'items': [
                {
                    'product_id': item['product_id'],
                    'product_name': item['product_name'],
                    'price': float(item['price']),
                    'quantity': random.randint(1, 3)
                }
                for _, item in order_items.iterrows()
            ]
        }
        
        return order
    
    def generate_page_view_event(self):
        """Генерация события просмотра страницы"""
        user = self.users_df.sample(1).iloc[0]
        
        event = {
            'event_id': str(uuid.uuid4()),
            'session_id': f"SES_{uuid.uuid4().hex[:12]}",
            'user_id': user['user_id'],
            'event_timestamp': datetime.now().isoformat(),
            'event_type': 'page_view',
            'page_url': f"/{random.choice(['home', 'products', 'cart', 'checkout', 'profile'])}",
            'referrer': random.choice(['google', 'facebook', 'direct', 'email', 'twitter']),
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
            'load_time_ms': random.randint(100, 5000),
            'country': user['country']
        }
        
        return event
    
    def run(self, events_per_second=10, duration_seconds=None):
        """Запуск генерации потока событий"""
        print("=" * 60)
        print("🚀 KAFKA EVENT PRODUCER")
        print("=" * 60)
        print(f"Bootstrap: {self.conf['bootstrap.servers']}")
        print(f"Rate: ~{events_per_second} events/sec")
        if duration_seconds:
            print(f"Duration: {duration_seconds}s")
        else:
            print("Duration: Infinite (Ctrl+C to stop)")
        
        # Генерация справочников
        self.generate_base_data()
        
        print("\n📡 Starting event stream...")
        print(f"   Topics:")
        for key, topic in self.topics.items():
            print(f"   - {topic}")
        
        start_time = time.time()
        event_count = 0
        
        try:
            while True:
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break
                
                # Генерируем события
                rand = random.random()
                
                if rand < 0.7:  # 70% - пользовательские события
                    event = self.generate_user_event()
                    self.produce_event(
                        self.topics['user_events'],
                        event['user_id'],
                        event
                    )
                    
                    
                    if event['action'] in ['page_view', 'product_view']:
                        page_event = self.generate_page_view_event()
                        self.produce_event(
                            self.topics['page_views'],
                            page_event['user_id'],
                            page_event
                        )
                
                elif rand < 0.85:  # 15% - заказы
                    order = self.generate_order_event()
                    self.produce_event(
                        self.topics['orders'],
                        order['user_id'],
                        order
                    )
                
                else:  # 15% - другие действия
                    event = self.generate_user_event()
                    self.produce_event(
                        self.topics['user_actions'],
                        event['user_id'],
                        event
                    )
                
                event_count += 1
                
                if event_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed if elapsed > 0 else 0
                    print(f"📊 Sent: {event_count:,} events in {elapsed:.1f}s ({rate:.1f} events/sec)")
                
                # Контроль скорости
                time.sleep(1.0 / events_per_second)
                
        except KeyboardInterrupt:
            print("\n\n⏹️  Stopping producer...")
        
        finally:
            # Все накопленные сообщения из буфера в Kafka
            print(f"📤 Flushing remaining messages...")
            self.producer.flush(timeout=30)                # Ждем 30 секунд
            
            elapsed = time.time() - start_time
            print(f"\n✅ Producer finished:")
            print(f"   Total events: {event_count:,}")
            print(f"   Duration: {elapsed:.1f}s")
            print(f"   Average rate: {event_count/elapsed:.1f} events/sec")
    
    def create_topics(self):
        """Создание топиков"""
        from confluent_kafka.admin import AdminClient, NewTopic
        
        admin = AdminClient({'bootstrap.servers': self.conf['bootstrap.servers']}) # Клиент для управления Kafka
        
        topic_list = []
        for topic_name in self.topics.values():
            topic_list.append(NewTopic(
                topic=topic_name,
                num_partitions=3,
                replication_factor=1
            ))
        
        try:
            fs = admin.create_topics(topic_list)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"✓ Topic created: {topic}")
                except Exception as e:
                    if "already exists" in str(e):
                        print(f"  Topic already exists: {topic}")
                    else:
                        print(f"  Error creating {topic}: {e}")
        except Exception as e:
            print(f"Admin error: {e}")


if __name__ == "__main__":
    producer = ECommerceEventProducer()
    
    
    try:
        producer.create_topics()
    except:
        print("Topics already exist")
    
    # Запускаем генерацию: m событий/сек в течение n секунд
    producer.run(events_per_second=1000, duration_seconds=100)