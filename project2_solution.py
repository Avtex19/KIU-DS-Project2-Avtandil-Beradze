import sys
from pathlib import Path
from typing import Tuple
import shutil

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).parent


def ensure_inputs_exist() -> Tuple[Path, Path, Path]:
	data_dir = PROJECT_ROOT / "data"
	original_dir = data_dir / "original"
	cleaned_dir = data_dir / "cleaned"
	original_dir.mkdir(parents=True, exist_ok=True)
	cleaned_dir.mkdir(parents=True, exist_ok=True)

	customers_csv = original_dir / "customers.csv"
	products_csv = original_dir / "products.csv"
	transactions_csv = original_dir / "transactions.csv"

	root_customers = PROJECT_ROOT / "customers.csv"
	root_products = PROJECT_ROOT / "products.csv"
	root_transactions = PROJECT_ROOT / "transactions.csv"
	if not customers_csv.exists() and root_customers.exists():
		shutil.copyfile(root_customers, customers_csv)
	if not products_csv.exists() and root_products.exists():
		shutil.copyfile(root_products, products_csv)
	if not transactions_csv.exists() and root_transactions.exists():
		shutil.copyfile(root_transactions, transactions_csv)

	missing = [p.name for p in [customers_csv, products_csv, transactions_csv] if not p.exists()]
	if missing:
		raise FileNotFoundError(
			f"Missing input files in data/original: {', '.join(missing)}. "
			"Please run: python generate_project2_data.py (it writes to project root), "
			"then re-run this script to auto-copy into data/original."
		)
	return customers_csv, products_csv, transactions_csv


def load_data(customers_csv: Path, products_csv: Path, transactions_csv: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
	customers_df = pd.read_csv(customers_csv, dtype={"customer_id": "string", "name": "string", "email": "string", "registration_date": "string", "country": "string", "age": "string"})
	products_df = pd.read_csv(products_csv, dtype={"product_id": "string", "product_name": "string", "category": "string", "price": "string", "stock": "string"})
	transactions_df = pd.read_csv(transactions_csv, dtype={"transaction_id": "string", "customer_id": "string", "product_id": "string", "quantity": "string", "transaction_date": "string", "payment_method": "string"})
	return customers_df, products_df, transactions_df


def clean_customers(customers_df: pd.DataFrame) -> pd.DataFrame:
	df = customers_df.copy()

	df = df.drop_duplicates()

	df["country"] = df["country"].astype("string").str.strip()
	df["country"] = df["country"].replace({"USA": "United States", "US": "United States"})

	age_numeric = df["age"].astype("string").str.extract(r"(\d+)")[0].astype("Int64")
	df["age"] = age_numeric

	def synthesize_email(row: pd.Series) -> str:
		if pd.notna(row.get("email")) and str(row["email"]).strip() != "":
			return str(row["email"]).strip().lower()
		name = str(row.get("name") or "").strip().lower()
		customer_id = str(row.get("customer_id") or "").strip().lower()
		if name:
			parts = [p for p in name.replace("  ", " ").split(" ") if p]
			local = ".".join(parts[:2]) if len(parts) >= 2 else (parts[0] if parts else "user")
		else:
			local = "user"
		if customer_id:
			local = f"{local}.{customer_id}"
		return f"{local}@example.com"

	df["email"] = df.apply(synthesize_email, axis=1)

	return df


def clean_products(products_df: pd.DataFrame) -> pd.DataFrame:
	df = products_df.copy()

	df["product_name"] = df["product_name"].astype("string").str.strip()

	canonical_categories = {"electronics": "Electronics", "clothing": "Clothing", "books": "Books", "home": "Home", "sports": "Sports"}
	df["category"] = df["category"].astype("string").str.strip().str.lower().map(canonical_categories).fillna("Other")

	df["price"] = pd.to_numeric(df["price"], errors="coerce")
	df.loc[df["price"] < 0, "price"] = np.nan
	category_median_price = df.groupby("category")["price"].transform(lambda s: s.fillna(s.median()))
	global_median_price = df["price"].median()
	df["price"] = df["price"].fillna(category_median_price).fillna(global_median_price)

	df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
	df.loc[df["stock"] > 1000, "stock"] = 1000
	df.loc[df["stock"] < 0, "stock"] = 0

	return df


def clean_transactions(transactions_df: pd.DataFrame, valid_customer_ids: pd.Series) -> pd.DataFrame:
	df = transactions_df.copy()

	if "transaction_id" in df.columns:
		df = df.drop_duplicates(subset=["transaction_id"], keep="first")
	else:
		df = df.drop_duplicates()

	df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
	df["quantity"] = df["quantity"].fillna(1).astype(int)
	df.loc[df["quantity"] < 1, "quantity"] = 1

	df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce", utc=False)
	cutoff = pd.Timestamp("2024-12-31")
	mask_future = df["transaction_date"] > cutoff
	df.loc[mask_future, "transaction_date"] = cutoff

	method_map = {"CREDIT CARD": "Credit Card", "PAYPAL": "PayPal", "BANK TRANSFER": "Bank Transfer"}
	df["payment_method"] = df["payment_method"].astype("string").str.strip().str.upper().map(method_map).fillna("Other")

	df = df[df["customer_id"].isin(valid_customer_ids)]

	return df


def compute_analytics(clean_transactions: pd.DataFrame, clean_products: pd.DataFrame, clean_customers: pd.DataFrame) -> dict:
	merged = (
		clean_transactions.merge(clean_products[["product_id", "price", "category"]], on="product_id", how="left")
		.merge(clean_customers[["customer_id", "country"]], on="customer_id", how="left")
	)
	merged["revenue"] = merged["quantity"] * merged["price"]

	kpis = {}
	kpis["total_revenue"] = float(merged["revenue"].sum())
	kpis["avg_order_value"] = float(merged["revenue"].mean())

	revenue_by_category = merged.groupby("category", dropna=False)["revenue"].sum().sort_values(ascending=False).reset_index()
	revenue_by_country = merged.groupby("country", dropna=False)["revenue"].sum().sort_values(ascending=False).reset_index()
	top_customers = merged.groupby("customer_id", dropna=False)["revenue"].sum().nlargest(5).reset_index()
	monthly_revenue = (
		merged.assign(month=merged["transaction_date"].dt.to_period("M").dt.to_timestamp())
		.groupby("month")["revenue"].sum().reset_index().sort_values("month")
	)
	payment_share = merged.groupby("payment_method", dropna=False)["revenue"].sum().sort_values(ascending=False).reset_index()

	return {
		"merged": merged,
		"revenue_by_category": revenue_by_category,
		"revenue_by_country": revenue_by_country,
		"top_customers": top_customers,
		"monthly_revenue": monthly_revenue,
		"payment_share": payment_share,
		"kpis": kpis,
	}


def save_outputs(clean_customers_df: pd.DataFrame, clean_products_df: pd.DataFrame, clean_transactions_df: pd.DataFrame, analytics: dict) -> None:
	clean_dir = PROJECT_ROOT / "data" / "cleaned"
	clean_dir.mkdir(parents=True, exist_ok=True)
	clean_customers_df.to_csv(clean_dir / "customers_clean.csv", index=False)
	clean_products_df.to_csv(clean_dir / "products_clean.csv", index=False)
	clean_transactions_df.to_csv(clean_dir / "transactions_clean.csv", index=False)

	output_dir = PROJECT_ROOT / "outputs"
	output_dir.mkdir(exist_ok=True)
	analytics["revenue_by_category"].to_csv(output_dir / "revenue_by_category.csv", index=False)
	analytics["revenue_by_country"].to_csv(output_dir / "revenue_by_country.csv", index=False)
	analytics["top_customers"].to_csv(output_dir / "top_customers.csv", index=False)
	analytics["monthly_revenue"].to_csv(output_dir / "monthly_revenue.csv", index=False)
	analytics["payment_share"].to_csv(output_dir / "payment_share.csv", index=False)


def print_summary(clean_customers_df: pd.DataFrame, clean_products_df: pd.DataFrame, clean_transactions_df: pd.DataFrame, analytics: dict) -> None:
	print("=" * 60)
	print("Cleaned Data Summary")
	print("=" * 60)
	print(f"data/cleaned/customers_clean.csv: {len(clean_customers_df)} rows")
	print(f"data/cleaned/products_clean.csv:  {len(clean_products_df)} rows")
	print(f"data/cleaned/transactions_clean.csv: {len(clean_transactions_df)} rows")
	print()
	print("=" * 60)
	print("KPIs")
	print("=" * 60)
	print(f"Total Revenue:         {analytics['kpis']['total_revenue']:.2f}")
	print(f"Average Order Value:   {analytics['kpis']['avg_order_value']:.2f}")
	print()
	print("Top 5 Customers by Revenue:")
	print(analytics["top_customers"].to_string(index=False))
	print()
	print("Revenue by Category:")
	print(analytics["revenue_by_category"].to_string(index=False))
	print()
	print("Monthly Revenue:")
	print(analytics["monthly_revenue"].to_string(index=False))
	print()
	print("Payment Method Share:")
	print(analytics["payment_share"].to_string(index=False))
	print()
	print("Outputs saved in: outputs/")


def main() -> int:
	try:
		customers_csv, products_csv, transactions_csv = ensure_inputs_exist()
	except FileNotFoundError as e:
		print(str(e))
		return 1

	customers_df, products_df, transactions_df = load_data(customers_csv, products_csv, transactions_csv)

	clean_customers_df = clean_customers(customers_df)
	clean_products_df = clean_products(products_df)
	clean_transactions_df = clean_transactions(transactions_df, valid_customer_ids=clean_customers_df["customer_id"])

	analytics = compute_analytics(clean_transactions_df, clean_products_df, clean_customers_df)
	save_outputs(clean_customers_df, clean_products_df, clean_transactions_df, analytics)
	print_summary(clean_customers_df, clean_products_df, clean_transactions_df, analytics)
	return 0


if __name__ == "__main__":
	sys.exit(main())


