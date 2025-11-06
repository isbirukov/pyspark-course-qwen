import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

def generate_sample_data(filename, rows=1000):
    """Генерирует тестовые данные для PySpark практики"""
    
    # Создаем директорию, если не существует
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Генерируем данные
    data = {
        'id': range(1, rows + 1),
        'name': [f'User_{i}' for i in range(1, rows + 1)],
        'age': [random.randint(18, 80) for _ in range(rows)],
        'salary': [random.randint(30000, 150000) for _ in range(rows)],
        'department': [random.choice(['IT', 'HR', 'Finance', 'Marketing', 'Sales']) for _ in range(rows)],
        'hire_date': [(datetime.now() - timedelta(days=random.randint(0, 365*5))).strftime('%Y-%m-%d') for _ in range(rows)],
        'is_active': [random.choice([True, False]) for _ in range(rows)]
    }
    
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Generated {rows} rows in {filename}")

def generate_sales_data(filename, rows=1000):
    """Генерирует тестовые данные продаж для E-commerce проекта"""
    
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    products = ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Mouse', 'Keyboard', 'Monitor', 'Speaker']
    
    data = {
        'order_id': range(1, rows + 1),
        'product': [random.choice(products) for _ in range(rows)],
        'quantity': [random.randint(1, 10) for _ in range(rows)],
        'price': [random.uniform(10.0, 2000.0) for _ in range(rows)],
        'customer_id': [random.randint(1, 500) for _ in range(rows)],
        'order_date': [(datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d') for _ in range(rows)],
        'region': [random.choice(['North', 'South', 'East', 'West']) for _ in range(rows)]
    }
    
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Generated {rows} sales records in {filename}")

if __name__ == "__main__":
    # Генерируем тестовые файлы
    generate_sample_data('../data/employees.csv', 10000)
    generate_sales_data('../data/sales.csv', 15000)
    print("Sample data generation completed!")
