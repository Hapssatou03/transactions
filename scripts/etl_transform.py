"""
ETL Transform Script
- Reads a raw CSV of transactions
- Cleans/normalizes columns
- Writes a cleaned CSV ready for Athena/partitioning
"""
import pandas as pd
import numpy as np
import argparse

REQUIRED_COLS = [
    "transaction_id","account_id","transaction_ts","type","amount","currency",
    "category","merchant","payment_method","status","city","country","ingested_at"
]

def fix_datetime(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace("/", "-", regex=False)
         .pipe(pd.to_datetime, errors="coerce")
    )

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:

    defaults = {
        "transaction_id": lambda: pd.NA,
        "account_id": lambda: pd.NA,
        "transaction_ts": lambda: pd.NaT,
        "type": lambda: pd.NA,
        "amount": lambda: pd.NA,
        "currency": lambda: "EUR",
        "category": lambda: "Unknown",
        "merchant": lambda: "Unknown",
        "payment_method": lambda: "Unknown",
        "status": lambda: "Completed",
        "city": lambda: pd.NA,
        "country": lambda: "FR",
        "ingested_at": lambda: pd.Timestamp.utcnow(),
    }
    for c, factory in defaults.items():
        if c not in df.columns:
            df[c] = factory()
    return df

def main(input_csv: str, output_csv: str):
    df = pd.read_csv(input_csv)

    # Dates
    df = ensure_cols(df)
    df["transaction_ts"] = fix_datetime(df["transaction_ts"])
    df = df.dropna(subset=["transaction_ts"])

    df["category"] = df["category"].astype(str).str.strip().str.title()
    df["merchant"] = df["merchant"].fillna("Unknown")

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["amount"])

    allowed = {"EUR", "USD", "GBP"}
    df["currency"] = df["currency"].astype(str).str.upper()
    df = df[df["currency"].isin(allowed)]

    if "ingested_at" not in df.columns or df["ingested_at"].isna().all():
        df["ingested_at"] = pd.Timestamp.utcnow()

    df = df[REQUIRED_COLS].sort_values("transaction_ts")

    df.to_csv(output_csv, index=False)
    print(f"Written cleaned CSV to: {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    main(args.input, args.output)
