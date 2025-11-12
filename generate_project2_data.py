"""
Project 2: Pandas Data Manipulation - Data Generator
Kutaisi International University
Introduction to Data Science with Python

This script generates realistic e-commerce data with intentional quality issues
for students to practice data cleaning and manipulation.

Instructions:
1. Run this script to generate three CSV files
2. Use these files for Tasks 1 and 2 of Project 2
3. Do not modify this script or the generated CSV files before starting your work

Generated files:
- customers.csv (200 rows with data quality issues)
- products.csv (50 rows with data quality issues)
- transactions.csv (500 rows with data quality issues)
"""

import pandas as pd
import numpy as np
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

print("=" * 60)
print("Project 2: Data Generator")
print("Introduction to Data Science with Python")
print("=" * 60)
print()

# ============================================================================
# PART 1: Generate Clean Base Data
# ============================================================================

print("[1/6] Generating customer data...")

# Customer data
customer_ids = [f"C{str(i + 1).zfill(3)}" for i in range(200)]
first_names = [
    "John",
    "Emma",
    "Michael",
    "Sophia",
    "William",
    "Olivia",
    "James",
    "Ava",
    "Oliver",
    "Isabella",
    "Liam",
    "Mia",
    "Noah",
    "Charlotte",
    "Ethan",
    "Amelia",
    "Lucas",
    "Harper",
    "Mason",
    "Evelyn",
    "Logan",
    "Abigail",
    "Alexander",
    "Emily",
]
last_names = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Thompson",
    "White",
]

countries = [
    "United States",
    "United Kingdom",
    "Germany",
    "France",
    "Italy",
    "Spain",
    "Canada",
    "Australia",
    "Japan",
    "Netherlands",
]

customers_data = {
    "customer_id": customer_ids,
    "name": [
        f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(200)
    ],
    "email": [
        f"{first_names[i % len(first_names)].lower()}.{last_names[i % len(last_names)].lower()}{i}@email.com"
        for i in range(200)
    ],
    "registration_date": pd.date_range(
        start="2024-01-01", end="2024-12-31", periods=200
    ).strftime("%Y-%m-%d"),
    "country": np.random.choice(countries, 200),
    "age": np.random.randint(18, 76, 200),
}

customers_df = pd.DataFrame(customers_data)

print("[2/6] Generating product data...")

# Product data
product_ids = [f"P{str(i + 1).zfill(3)}" for i in range(50)]
categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
product_names = {
    "Electronics": [
        "Laptop",
        "Smartphone",
        "Tablet",
        "Headphones",
        "Camera",
        "Monitor",
        "Keyboard",
        "Mouse",
        "Speaker",
        "Smartwatch",
    ],
    "Clothing": [
        "T-Shirt",
        "Jeans",
        "Jacket",
        "Dress",
        "Sweater",
        "Shoes",
        "Sneakers",
        "Coat",
        "Hat",
        "Scarf",
    ],
    "Books": [
        "Fiction Novel",
        "Science Book",
        "Biography",
        "Cookbook",
        "Travel Guide",
        "History Book",
        "Self-Help",
        "Mystery Novel",
        "Fantasy Book",
        "Poetry Collection",
    ],
    "Home": [
        "Lamp",
        "Pillow",
        "Blanket",
        "Vase",
        "Picture Frame",
        "Candle",
        "Rug",
        "Clock",
        "Mirror",
        "Plant Pot",
    ],
    "Sports": [
        "Yoga Mat",
        "Dumbbells",
        "Running Shoes",
        "Tennis Racket",
        "Basketball",
        "Bicycle",
        "Swim Goggles",
        "Fitness Tracker",
        "Jump Rope",
        "Water Bottle",
    ],
}

products_list = []
for _ in range(50):
    category = random.choice(categories)
    product_name = random.choice(product_names[category])
    products_list.append(
        {
            "product_id": product_ids[len(products_list)],
            "product_name": product_name,
            "category": category,
            "price": round(np.random.uniform(10, 500), 2),
            "stock": np.random.randint(0, 101),
        }
    )

products_df = pd.DataFrame(products_list)

print("[3/6] Generating transaction data...")

# Transaction data
transaction_ids = [f"T{str(i + 1).zfill(3)}" for i in range(500)]
payment_methods = ["Credit Card", "PayPal", "Bank Transfer"]

transactions_data = {
    "transaction_id": transaction_ids,
    "customer_id": np.random.choice(customer_ids, 500),
    "product_id": np.random.choice(product_ids, 500),
    "quantity": np.random.randint(1, 6, 500),
    "transaction_date": pd.date_range(
        start="2024-01-01", end="2024-12-31", periods=500
    ).strftime("%Y-%m-%d"),
    "payment_method": np.random.choice(payment_methods, 500),
}

transactions_df = pd.DataFrame(transactions_data)

# ============================================================================
# PART 2: Introduce Data Quality Issues
# ============================================================================

print("[4/6] Introducing data quality issues in customers...")

# CUSTOMERS ISSUES
# 1. Missing emails (10%)
missing_email_indices = np.random.choice(customers_df.index, size=20, replace=False)
customers_df.loc[missing_email_indices, "email"] = np.nan

# 2. Duplicate rows (5 exact duplicates)
duplicate_indices = np.random.choice(customers_df.index, size=5, replace=False)
duplicates = customers_df.loc[duplicate_indices].copy()
customers_df = pd.concat([customers_df, duplicates], ignore_index=True)

# 3. Age as mixed types (some strings)
string_age_indices = np.random.choice(customers_df.index, size=15, replace=False)
for idx in string_age_indices:
    customers_df.at[idx, "age"] = f"{customers_df.at[idx, 'age']} years"

# 4. Inconsistent country names
us_indices = customers_df[customers_df["country"] == "United States"].index
if len(us_indices) > 0:
    # Replace some with "USA" and "US"
    usa_indices = np.random.choice(
        us_indices, size=min(10, len(us_indices)), replace=False
    )
    us_short_indices = np.random.choice(
        us_indices, size=min(8, len(us_indices)), replace=False
    )
    customers_df.loc[usa_indices, "country"] = "USA"
    customers_df.loc[us_short_indices, "country"] = "US"

print("[5/6] Introducing data quality issues in products...")

# PRODUCTS ISSUES
# 1. Missing prices (5%)
missing_price_indices = np.random.choice(products_df.index, size=3, replace=False)
products_df.loc[missing_price_indices, "price"] = np.nan

# 2. Negative prices (3 products)
negative_price_indices = np.random.choice(products_df.index, size=3, replace=False)
products_df.loc[negative_price_indices, "price"] = -products_df.loc[
    negative_price_indices, "price"
]

# 3. Unrealistic stock values
high_stock_indices = np.random.choice(products_df.index, size=3, replace=False)
products_df.loc[high_stock_indices, "stock"] = np.random.randint(5000, 15000, 3)

# 4. Whitespace in product names
whitespace_indices = np.random.choice(products_df.index, size=10, replace=False)
for idx in whitespace_indices:
    products_df.at[idx, "product_name"] = f"  {products_df.at[idx, 'product_name']}  "

# 5. Mixed case in categories
mixed_case_indices = np.random.choice(products_df.index, size=8, replace=False)
for idx in mixed_case_indices:
    products_df.at[idx, "category"] = products_df.at[idx, "category"].lower()

print("[6/6] Introducing data quality issues in transactions...")

# TRANSACTIONS ISSUES
# 1. Missing quantities (3%)
missing_qty_indices = np.random.choice(transactions_df.index, size=15, replace=False)
transactions_df.loc[missing_qty_indices, "quantity"] = np.nan

# 2. Duplicate transactions (same transaction_id)
dup_trans_indices = np.random.choice(transactions_df.index, size=8, replace=False)
duplicates_trans = transactions_df.loc[dup_trans_indices].copy()
transactions_df = pd.concat([transactions_df, duplicates_trans], ignore_index=True)

# 3. Invalid customer_id references
invalid_cust_indices = np.random.choice(transactions_df.index, size=10, replace=False)
transactions_df.loc[invalid_cust_indices, "customer_id"] = [
    "C999",
    "C998",
    "C997",
    "C996",
    "C995",
    "C994",
    "C993",
    "C992",
    "C991",
    "C990",
]

# 4. Future dates
future_date_indices = np.random.choice(transactions_df.index, size=5, replace=False)
future_dates = pd.date_range(start="2025-01-01", end="2025-12-31", periods=5).strftime(
    "%Y-%m-%d"
)
transactions_df.loc[future_date_indices, "transaction_date"] = future_dates

# 5. Mixed case in payment methods
mixed_payment_indices = np.random.choice(transactions_df.index, size=20, replace=False)
for idx in mixed_payment_indices:
    transactions_df.at[idx, "payment_method"] = transactions_df.at[
        idx, "payment_method"
    ].upper()

# ============================================================================
# PART 3: Save to CSV files
# ============================================================================

print()
print("=" * 60)
print("Saving CSV files...")
print("=" * 60)

customers_df.to_csv("customers.csv", index=False)
print("✓ customers.csv saved (205 rows including duplicates)")

products_df.to_csv("products.csv", index=False)
print("✓ products.csv saved (50 rows)")

transactions_df.to_csv("transactions.csv", index=False)
print("✓ transactions.csv saved (508 rows including duplicates)")

# ============================================================================
# PART 4: Print Data Quality Summary
# ============================================================================

print()
print("=" * 60)
print("Data Quality Issues Summary")
print("=" * 60)
print()
print("CUSTOMERS.CSV:")
print(f"  • Total rows: {len(customers_df)}")
print(f"  • Missing emails: {customers_df['email'].isna().sum()}")
print(f"  • Duplicate rows: ~5")
print(f"  • Age with 'years' string: ~15")
print(f"  • Inconsistent country names: USA/US instead of United States")
print()
print("PRODUCTS.CSV:")
print(f"  • Total rows: {len(products_df)}")
print(f"  • Missing prices: {products_df['price'].isna().sum()}")
print(f"  • Negative prices: 3")
print(f"  • Unrealistic stock (>1000): {(products_df['stock'] > 1000).sum()}")
print(f"  • Product names with whitespace: ~10")
print(f"  • Mixed case categories: ~8")
print()
print("TRANSACTIONS.CSV:")
print(f"  • Total rows: {len(transactions_df)}")
print(f"  • Missing quantities: {transactions_df['quantity'].isna().sum()}")
print(f"  • Duplicate transaction_ids: ~8")
print(f"  • Invalid customer_id references: 10")
print(f"  • Future dates: 5")
print(f"  • Mixed case payment methods: ~20")
print()
print("=" * 60)
print("Data generation complete!")
print("You can now proceed with Project 2 Tasks 1-3")
print("=" * 60)
print()
print("NOTE: Do NOT modify these CSV files before starting your project.")
print("      Your cleaning work should be done in your project code.")
print("=" * 60)
