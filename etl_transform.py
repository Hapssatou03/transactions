"""
ETL Transform Script (local example)
- Reads a raw CSV of transactions
- Cleans columns (dates, categories, null merchants)
- Writes a cleaned CSV (ready for Athena)
"""
import pandas as pd
import numpy as np
from datetime import datetime
import argparse

def fix_datetime(s: pd.Series) -> pd.Series:
    # Accept both "YYYY-MM-DD HH:MM:SS" and "YYYY/MM/DD HH:MM:SS"
    return (s
            .str.replace("/", "-", regex=False)
            .pipe(pd.to_datetime, errors="coerce"))

def main(input_csv: str, output_csv: str):
    df = pd.read_csv(input_csv, dtype={})
    # Normalize datetime
    df["transaction_ts"] = fix_datetime(df["transaction_ts"])
    # Drop rows with invalid timestamps
    df = df.dropna(subset=["transaction_ts"])

    # Normalize category casing
    df["category"] = df["category"].astype(str).str.strip().str.title()

    # Fill missing merchants with "Unknown"
    df["merchant"] = df["merchant"].fillna("Unknown")

    # Ensure amount is float
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["amount"])

    # Ensure currency is uppercase and in allowed set
    allowed = {"EUR","USD","GBP"}
    df["currency"] = df["currency"].astype(str).str.upper()
    df = df[df["currency"].isin(allowed)]

    # Reorder columns
    cols = [
        "transaction_id","account_id","transaction_ts","type","amount","currency",
        "category","merchant","payment_method","status","city","country","ingested_at"
    ]
    df = df[cols].sort_values("transaction_ts")

    df.to_csv(output_csv, index=False)
    print(f"Written cleaned CSV to: {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    main(args.input, args.output)
