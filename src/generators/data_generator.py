from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import json
import os
import uuid

class ECommerceDataGenerator:
    """Генератор синтетических данных e-commerce платформы (без numpy)"""
    
    def __init__(self, seed=42):
        self.fake = Faker()
        random.seed(seed)
        Faker.seed(seed)
        
        self.product_categories = {
            'electronics': ['laptop', 'smartphone', 'tablet', 'headphones', 'camera'],
            'clothing': ['shirt', 'pants', 'dress', 'jacket', 'shoes'],
            'books': ['fiction', 'non-fiction', 'textbook', 'comic', 'magazine'],
            'home': ['furniture', 'kitchen', 'decor', 'garden', 'lighting']
        }
        
        self.price_ranges = {
            'electronics': (100, 3000),
            'clothing': (20, 500),
            'books': (5, 200),
            'home': (50, 2000)
        }
    
    def generate_products(self, num_products=1000):
        """Генерация каталога товаров (без numpy)"""
        products = []
        categories_list = list(self.product_categories.keys())
        
        for i in range(num_products):
            
            category = random.choice(categories_list)
            subcategory = random.choice(self.product_categories[category])
            price_range = self.price_ranges[category]
            
            # Вместо np.random.uniform используем random.uniform
            price = round(random.uniform(*price_range), 2)
            cost = round(random.uniform(price_range[0]*0.4, price_range[1]*0.7), 2)
            
            # Вместо np.random.randint используем random.randint
            stock_quantity = random.randint(0, 1000)
            
            # Вместо np.random.uniform для rating
            rating = round(random.uniform(1, 5), 1)
            
            product = {
                'product_id': f"PROD_{i:06d}",
                'product_name': f"{subcategory}_{self.fake.word()}_{i}",
                'category': category,
                'subcategory': subcategory,
                'price': price,
                'cost': cost,
                'stock_quantity': stock_quantity,
                'rating': rating,
                'created_date': self.fake.date_between(start_date='-2y', end_date='today')
            }
            products.append(product)
        
        return pd.DataFrame(products)
    
    def generate_users(self, num_users=10000):
        """Генерация профилей пользователей (без numpy)"""
        users = []
        age_groups = ['18-24', '25-34', '35-44', '45-54', '55+']
        membership_levels = ['free', 'basic', 'premium', 'vip']
        membership_weights = [0.4, 0.3, 0.2, 0.1]
        
        for i in range(num_users):
            user = {
                'user_id': f"USER_{i:07d}",
                'username': f"{self.fake.user_name()}_{i}",
                'email': f"{self.fake.user_name()}_{i}@{self.fake.free_email_domain()}",
                'registration_date': self.fake.date_between(start_date='-2y', end_date='today'),
                'country': self.fake.country(),
                'city': self.fake.city(),
                'age_group': random.choice(age_groups),
                'membership_level': random.choices(membership_levels, weights=membership_weights)[0]
            }
            users.append(user)
        
        return pd.DataFrame(users)
    
    def generate_sessions(self, products_df, users_df, num_sessions=10000, 
                          start_date='2024-01-01', end_date='2024-12-31'):
        """Генерация пользовательских сессий (без numpy)"""
        sessions = []
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        user_actions = ['page_view', 'product_view', 'add_to_cart', 
                       'remove_from_cart', 'checkout_start', 'purchase', 
                       'search', 'review_write']
        action_weights = [0.3, 0.25, 0.15, 0.05, 0.1, 0.05, 0.07, 0.03]
        devices = ['mobile', 'desktop', 'tablet']
        device_weights = [0.5, 0.4, 0.1]
        browsers = ['chrome', 'firefox', 'safari', 'edge']
        
        total_seconds = int((end - start).total_seconds())
        
        for i in range(num_sessions):
            session_id = f"SES_{uuid.uuid4().hex[:12]}"
            user = users_df.sample(1).iloc[0]
            
            
            session_start = start + timedelta(
                seconds=random.randint(0, total_seconds)
            )
            
            
            num_events = random.randint(1, 15)
            current_time = session_start
            
            for j in range(num_events):
                # Вместо np.random.choice с вероятностями используем random.choices
                action = random.choices(user_actions, weights=action_weights)[0]
                
                # Вместо np.random.random используем random.random
                product = products_df.sample(1).iloc[0] if random.random() > 0.3 else None
                
                event = {
                    'session_id': session_id,
                    'user_id': user['user_id'],
                    'event_timestamp': current_time.isoformat(),
                    'action': action,
                    'product_id': product['product_id'] if product is not None else None,
                    'category': product['category'] if product is not None else None,
                    'page_url': f"/{random.choice(['home', 'products', 'cart', 'checkout', 'profile'])}",
                    'device': random.choices(devices, weights=device_weights)[0],
                    'browser': random.choice(browsers),
                    'ip_address': self.fake.ipv4(),
                    'session_duration_sec': random.randint(1, 3600)
                }
                sessions.append(event)
                
                
                current_time += timedelta(seconds=random.randint(1, 300))
        
        return pd.DataFrame(sessions)
    
    def generate_orders(self, sessions_df, products_df, users_df):
        """Генерация заказов на основе успешных покупок (без numpy)"""
        purchases = sessions_df[sessions_df['action'] == 'purchase'].copy()
        
        orders = []
        order_statuses = ['completed', 'processing', 'cancelled']
        status_weights = [0.8, 0.15, 0.05]
        payment_methods = ['credit_card', 'paypal', 'crypto', 'bank_transfer']
        
        for _, purchase in purchases.iterrows():
            num_items = random.randint(1, 5)
            order_items = products_df.sample(num_items)
            
            order = {
                'order_id': f"ORD_{self.fake.uuid4()[:8]}",
                'user_id': purchase['user_id'],
                'session_id': purchase['session_id'],
                'order_timestamp': purchase['event_timestamp'],
                'items_count': num_items,
                'total_amount': round(order_items['price'].sum(), 2),
                'discount_applied': round(random.uniform(0, 0.3), 2),
                'shipping_cost': round(random.uniform(0, 20), 2),
                'payment_method': random.choice(payment_methods),
                'order_status': random.choices(order_statuses, weights=status_weights)[0]
            }
            orders.append(order)
        
        return pd.DataFrame(orders)
    
    def save_data(self, data, path, format='json'):
        """Сохранение данных в разных форматах"""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        if format == 'json':
            data.to_json(path, orient='records', lines=True)
        elif format == 'csv':
            data.to_csv(path, index=False)
        elif format == 'parquet':
            data.to_parquet(path, index=False)
        
        print(f"Saved {len(data)} records to {path}")