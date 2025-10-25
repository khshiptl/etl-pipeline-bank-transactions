import pandas as pd
from sqlalchemy import create_engine

# -------------------------------
# 1. Extract
# -------------------------------
print("Starting ETL process...")

# Read the CSV file
df = pd.read_csv("bank_transactions.csv")
print("Data extracted successfully.")
print("Initial shape:", df.shape)

# -------------------------------
# 2. Transform
# -------------------------------

# Remove duplicates
df = df.drop_duplicates()

# Standardize column names
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

# Remove rows with missing values
df = df.dropna()

# Remove invalid transaction amounts
df = df[df["transactionamount_(inr)"] > 0]

# Convert transaction date to datetime
df["transactiondate"] = pd.to_datetime(df["transactiondate"], errors="coerce")

# Drop any rows that failed date conversion
df = df.dropna(subset=["transactiondate"])

print("Data transformed successfully.")
print("Cleaned shape:", df.shape)

# -------------------------------
# 3. Load
# -------------------------------

# Connect to local PostgreSQL
username = "khushipatel"
password = ""  # local connection, no password
database = "bank_transactions_db"

connection_string = f"postgresql+psycopg2://{username}@localhost:5432/{database}"
engine = create_engine(connection_string)

# Load data into SQL table
df.to_sql("transactions", con=engine, if_exists="replace", index=False)
print("Data loaded successfully into PostgreSQL.")

# Verify row count
from sqlalchemy import text
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM transactions;"))
    print("Row count in database:", result.scalar())

print("ETL process completed successfully.")

