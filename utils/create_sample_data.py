"""
Sample Data Generator
Creates realistic retail data for development and testing
"""

import os
import random
from datetime import datetime, timedelta
from typing import List
import sqlite3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Seed for reproducibility
random.seed(42)


def create_sample_data(db_path: str = "data/retail_lakehouse.db"):
    """Create sample retail data in SQLite database"""
    
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    logger.info("Creating tables...")
    
    # =========================================================================
    # Create Tables
    # =========================================================================
    
    # Stores
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores_locations (
            store_id INTEGER PRIMARY KEY,
            store_name TEXT NOT NULL,
            region TEXT NOT NULL,
            state TEXT NOT NULL,
            city TEXT NOT NULL,
            address TEXT,
            store_type TEXT,
            square_footage INTEGER,
            opened_date DATE,
            manager_name TEXT
        )
    """)
    
    # Products
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products_catalog (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            product_line TEXT NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT,
            price_tier TEXT NOT NULL,
            unit_price REAL NOT NULL,
            unit_cost REAL NOT NULL,
            brand TEXT,
            supplier_id INTEGER,
            is_active BOOLEAN DEFAULT 1
        )
    """)
    
    # Customers
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers_profiles (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            loyalty_tier TEXT,
            signup_date DATE,
            lifetime_value REAL,
            preferred_store_id INTEGER,
            region TEXT
        )
    """)
    
    # Sales Transactions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales_transactions (
            transaction_id INTEGER PRIMARY KEY,
            transaction_date DATE NOT NULL,
            transaction_time TEXT,
            store_id INTEGER NOT NULL,
            customer_id INTEGER,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            discount_amount REAL DEFAULT 0,
            revenue REAL NOT NULL,
            payment_method TEXT,
            FOREIGN KEY (store_id) REFERENCES stores_locations(store_id),
            FOREIGN KEY (customer_id) REFERENCES customers_profiles(customer_id),
            FOREIGN KEY (product_id) REFERENCES products_catalog(product_id)
        )
    """)
    
    # Inventory
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS inventory_levels (
            inventory_id INTEGER PRIMARY KEY,
            store_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity_on_hand INTEGER NOT NULL,
            quantity_reserved INTEGER DEFAULT 0,
            reorder_point INTEGER,
            last_updated TIMESTAMP,
            FOREIGN KEY (store_id) REFERENCES stores_locations(store_id),
            FOREIGN KEY (product_id) REFERENCES products_catalog(product_id)
        )
    """)
    
    # Marketing Campaigns
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS marketing_campaigns (
            campaign_id INTEGER PRIMARY KEY,
            campaign_name TEXT NOT NULL,
            campaign_type TEXT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE,
            target_region TEXT,
            target_product_line TEXT,
            budget REAL,
            actual_spend REAL,
            impressions INTEGER,
            conversions INTEGER,
            status TEXT
        )
    """)
    
    # Weather Data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            weather_id INTEGER PRIMARY KEY,
            date DATE NOT NULL,
            region TEXT NOT NULL,
            avg_temperature REAL,
            precipitation REAL,
            weather_condition TEXT,
            is_extreme BOOLEAN DEFAULT 0
        )
    """)
    
    # Competitors Pricing
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS competitors_pricing (
            pricing_id INTEGER PRIMARY KEY,
            date DATE NOT NULL,
            competitor_name TEXT NOT NULL,
            product_category TEXT NOT NULL,
            avg_price REAL,
            discount_percentage REAL,
            promotion_active BOOLEAN
        )
    """)
    
    conn.commit()
    
    # =========================================================================
    # Generate Data
    # =========================================================================
    
    logger.info("Generating stores...")
    
    regions = ["Northeast", "Southeast", "Midwest", "West"]
    states_by_region = {
        "Northeast": ["NY", "MA", "PA", "NJ", "CT"],
        "Southeast": ["FL", "GA", "NC", "SC", "VA"],
        "Midwest": ["IL", "OH", "MI", "WI", "MN"],
        "West": ["CA", "WA", "OR", "AZ", "CO"]
    }
    cities_by_state = {
        "NY": ["New York", "Buffalo", "Albany"],
        "MA": ["Boston", "Worcester", "Springfield"],
        "PA": ["Philadelphia", "Pittsburgh", "Allentown"],
        "FL": ["Miami", "Orlando", "Tampa"],
        "GA": ["Atlanta", "Savannah", "Augusta"],
        "IL": ["Chicago", "Springfield", "Peoria"],
        "CA": ["Los Angeles", "San Francisco", "San Diego"],
        "WA": ["Seattle", "Tacoma", "Spokane"],
    }
    
    stores = []
    store_id = 1
    for region in regions:
        for state in states_by_region[region]:
            cities = cities_by_state.get(state, ["City1", "City2"])
            for city in cities[:2]:  # 2 stores per city
                stores.append((
                    store_id,
                    f"RetailCo {city} #{store_id}",
                    region,
                    state,
                    city,
                    f"{random.randint(100, 9999)} Main St",
                    random.choice(["Standard", "Premium", "Outlet"]),
                    random.randint(15000, 80000),
                    (datetime.now() - timedelta(days=random.randint(365, 3650))).strftime("%Y-%m-%d"),
                    f"Manager {store_id}"
                ))
                store_id += 1
    
    cursor.executemany("""
        INSERT OR REPLACE INTO stores_locations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, stores)
    
    logger.info(f"Created {len(stores)} stores")
    
    # Products
    logger.info("Generating products...")
    
    product_lines = {
        "Electronics": ["Phones", "Laptops", "Tablets", "Accessories", "Audio"],
        "Apparel": ["Mens", "Womens", "Kids", "Footwear", "Accessories"],
        "Home": ["Furniture", "Decor", "Kitchen", "Bedding", "Storage"],
        "Outdoor": ["Camping", "Sports", "Garden", "Patio", "Fitness"],
        "Food": ["Snacks", "Beverages", "Grocery", "Frozen", "Fresh"]
    }
    
    price_tiers = ["Budget", "Standard", "Premium"]
    
    products = []
    product_id = 1
    for product_line, categories in product_lines.items():
        for category in categories:
            for tier in price_tiers:
                for i in range(10):  # 10 products per category/tier
                    base_price = {"Budget": 20, "Standard": 50, "Premium": 150}[tier]
                    price = base_price * random.uniform(0.5, 3.0)
                    products.append((
                        product_id,
                        f"{tier} {category} Item {i+1}",
                        product_line,
                        category,
                        f"Sub-{category}-{i % 3}",
                        tier,
                        round(price, 2),
                        round(price * random.uniform(0.4, 0.7), 2),
                        f"Brand-{random.randint(1, 20)}",
                        random.randint(1, 50),
                        1
                    ))
                    product_id += 1
    
    cursor.executemany("""
        INSERT OR REPLACE INTO products_catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, products)
    
    logger.info(f"Created {len(products)} products")
    
    # Customers
    logger.info("Generating customers...")
    
    loyalty_tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    
    customers = []
    for customer_id in range(1, 10001):
        region = random.choice(regions)
        signup_days_ago = random.randint(30, 1825)
        customers.append((
            customer_id,
            f"First{customer_id}",
            f"Last{customer_id}",
            f"customer{customer_id}@email.com",
            random.choices(loyalty_tiers, weights=[50, 30, 15, 5])[0],
            (datetime.now() - timedelta(days=signup_days_ago)).strftime("%Y-%m-%d"),
            round(random.uniform(100, 10000), 2),
            random.choice([s[0] for s in stores if s[2] == region]),
            region
        ))
    
    cursor.executemany("""
        INSERT OR REPLACE INTO customers_profiles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, customers)
    
    logger.info(f"Created {len(customers)} customers")
    
    # Sales Transactions (last 90 days)
    logger.info("Generating sales transactions...")
    
    payment_methods = ["Credit", "Debit", "Cash", "Mobile"]
    
    transactions = []
    transaction_id = 1
    
    # Create seasonal patterns
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    current_date = start_date
    
    while current_date <= end_date:
        # More transactions on weekends
        is_weekend = current_date.weekday() >= 5
        daily_transactions = random.randint(800, 1500) if is_weekend else random.randint(500, 1000)
        
        for _ in range(daily_transactions):
            store = random.choice(stores)
            product = random.choice(products)
            customer_id = random.choice([None, random.randint(1, 10000)])
            
            # Regional weather effect on outdoor products
            region = store[2]
            product_line = product[2]
            
            # Simulate declining outdoor sales in Northeast (for the example query)
            if region == "Northeast" and product_line == "Outdoor" and product[5] == "Premium":
                if current_date > end_date - timedelta(days=30):
                    # 15% decline in last month
                    if random.random() > 0.85:
                        continue
            
            quantity = random.randint(1, 5)
            unit_price = product[6]
            discount = round(unit_price * random.uniform(0, 0.2), 2) if random.random() > 0.7 else 0
            revenue = round(quantity * (unit_price - discount), 2)
            
            transactions.append((
                transaction_id,
                current_date.strftime("%Y-%m-%d"),
                f"{random.randint(8, 21):02d}:{random.randint(0, 59):02d}:00",
                store[0],
                customer_id,
                product[0],
                quantity,
                unit_price,
                discount,
                revenue,
                random.choice(payment_methods)
            ))
            transaction_id += 1
        
        current_date += timedelta(days=1)
    
    # Batch insert transactions
    cursor.executemany("""
        INSERT OR REPLACE INTO sales_transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, transactions)
    
    logger.info(f"Created {len(transactions)} transactions")
    
    # Weather Data
    logger.info("Generating weather data...")
    
    weather_conditions = ["Sunny", "Cloudy", "Rainy", "Snowy", "Windy"]
    
    weather = []
    weather_id = 1
    current_date = start_date
    
    while current_date <= end_date:
        for region in regions:
            # Base temperature by region
            base_temp = {"Northeast": 35, "Southeast": 55, "Midwest": 40, "West": 50}[region]
            
            # Simulate cold snap in Northeast (for example query)
            if region == "Northeast" and current_date > end_date - timedelta(days=30):
                base_temp -= 15  # Unseasonably cold
            
            temp = base_temp + random.uniform(-10, 10)
            
            weather.append((
                weather_id,
                current_date.strftime("%Y-%m-%d"),
                region,
                round(temp, 1),
                round(random.uniform(0, 2), 2),
                random.choice(weather_conditions),
                1 if temp < 20 or temp > 95 else 0
            ))
            weather_id += 1
        
        current_date += timedelta(days=1)
    
    cursor.executemany("""
        INSERT OR REPLACE INTO weather_data VALUES (?, ?, ?, ?, ?, ?, ?)
    """, weather)
    
    logger.info(f"Created {len(weather)} weather records")
    
    # Marketing Campaigns
    logger.info("Generating marketing campaigns...")
    
    campaigns = [
        (1, "Summer Outdoor Sale", "Discount", "2024-06-01", "2024-08-31", "All", "Outdoor", 50000, 48000, 1000000, 15000, "completed"),
        (2, "Fall Electronics Push", "Promotion", "2024-09-01", "2024-10-31", "All", "Electronics", 75000, 72000, 2000000, 25000, "completed"),
        (3, "Holiday Campaign", "Multi-channel", "2024-11-15", "2024-12-31", "All", None, 150000, 145000, 5000000, 75000, "completed"),
        (4, "Winter Clearance", "Discount", "2025-01-01", None, "All", "Apparel", 30000, 15000, 500000, 8000, "active"),
        # Note: No active outdoor campaign (for example query)
    ]
    
    cursor.executemany("""
        INSERT OR REPLACE INTO marketing_campaigns VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, campaigns)
    
    logger.info(f"Created {len(campaigns)} campaigns")
    
    # Competitor Pricing
    logger.info("Generating competitor pricing...")
    
    competitors = ["CompetitorA", "CompetitorB", "CompetitorC"]
    
    competitor_pricing = []
    pricing_id = 1
    current_date = start_date
    
    while current_date <= end_date:
        for competitor in competitors:
            for product_line in product_lines.keys():
                base_price = {"Electronics": 100, "Apparel": 40, "Home": 60, "Outdoor": 80, "Food": 15}[product_line]
                
                # Simulate competitor promotion on outdoor (for example query)
                discount = 0
                promo = False
                if competitor == "CompetitorA" and product_line == "Outdoor":
                    if current_date > end_date - timedelta(days=21):
                        discount = 20  # 20% off promotion
                        promo = True
                
                competitor_pricing.append((
                    pricing_id,
                    current_date.strftime("%Y-%m-%d"),
                    competitor,
                    product_line,
                    round(base_price * random.uniform(0.9, 1.1), 2),
                    discount,
                    promo
                ))
                pricing_id += 1
        
        current_date += timedelta(days=7)  # Weekly pricing data
    
    cursor.executemany("""
        INSERT OR REPLACE INTO competitors_pricing VALUES (?, ?, ?, ?, ?, ?, ?)
    """, competitor_pricing)
    
    logger.info(f"Created {len(competitor_pricing)} competitor pricing records")
    
    # Create indexes
    logger.info("Creating indexes...")
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON sales_transactions(transaction_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_store ON sales_transactions(store_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_product ON sales_transactions(product_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_line ON products_catalog(product_line)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_stores_region ON stores_locations(region)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_weather_date_region ON weather_data(date, region)")
    
    conn.commit()
    conn.close()
    
    logger.info(f"✓ Sample data created successfully in {db_path}")
    
    return {
        "stores": len(stores),
        "products": len(products),
        "customers": len(customers),
        "transactions": len(transactions),
        "weather_records": len(weather),
        "campaigns": len(campaigns)
    }


if __name__ == "__main__":
    stats = create_sample_data()
    print("\nData Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value:,}")
