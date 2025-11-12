## Project 2 — Pandas Data Manipulation

## Student

Name: Avtandil Beradze
Student ID: 60001155177

This repository contains:
- `generate_project2_data.py`: script to generate three CSVs with intentional data quality issues
- `project2_solution.py`: end-to-end cleaning and analysis solution using Pandas
- Data is organized for submission under `data/`:
  - `data/original/` → `customers.csv`, `products.csv`, `transactions.csv`
  - `data/cleaned/`  → `customers_clean.csv`, `products_clean.csv`, `transactions_clean.csv`
- Analytics tables are saved under `outputs/`

### Quick Start
1) Create the data
```
python generate_project2_data.py
```

2) Run the solution
```
python project2_solution.py
```

3) Inspect results
- Original CSVs (for grading): `data/original/`
- Cleaned CSVs (for grading): `data/cleaned/`
- Analytics in `outputs/`:
  - `revenue_by_category.csv`
  - `revenue_by_country.csv`
  - `top_customers.csv`
  - `monthly_revenue.csv`
  - `payment_share.csv`
- Console prints a summary with KPIs.

### Environment
Install dependencies (Python 3.10+ recommended):
```
pip install -r requirements.txt
```

### Submission Structure (include these in GitHub)
- `Project2_FirstName_LastName.py` (or `.ipynb`)
- `data/`
  - `original/` → `customers.csv`, `products.csv`, `transactions.csv`
  - `cleaned/` → `customers_clean.csv`, `products_clean.csv`, `transactions_clean.csv`
- `README.md`
- `requirements.txt`

### Cleaning Rules Implemented
- Customers
  - Drop exact duplicate rows
  - Normalize `country` values: map `USA`/`US` → `United States`
  - Convert `age` mixed types to numeric (strip non-digits)
  - Fill missing `email` by synthesizing from `name` + `customer_id`
- Products
  - Trim whitespace in `product_name`
  - Standardize `category` casing to one of: Electronics, Clothing, Books, Home, Sports (fallback `Other`)
  - Convert `price` to numeric; negatives → NaN; impute by category median (fallback to global median)
  - Convert `stock` to integer; cap unrealistic values > 1000 to 1000; floor < 0 to 0
- Transactions
  - Drop duplicate `transaction_id` (keep first)
  - Convert `quantity` to integer; missing or invalid → 1; enforce minimum 1
  - Parse `transaction_date` and clamp future dates to `2024-12-31`
  - Standardize `payment_method` to: Credit Card, PayPal, Bank Transfer (fallback `Other`)
  - Remove rows with invalid `customer_id` not present in cleaned customers

### Analytics Produced
- KPIs: total revenue, average order value
- Revenue by category and by country
- Top 5 customers by revenue
- Monthly revenue trend
- Revenue share by payment method

### Notes
- Always run `generate_project2_data.py` before the solution to refresh the raw CSVs. The solution will copy root CSVs into `data/original/` automatically if needed.
- Cleaning is deterministic given the generated data; random seeds are set by the generator script.


