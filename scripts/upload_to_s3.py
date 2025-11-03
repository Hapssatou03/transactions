"""
upload_to_s3.py
Uploads raw &/or cleaned transactions files to S3 with date-based keys.

Usage examples:
  python upload_to_s3.py --bucket hapssatou-datalake-123 --region eu-west-3 --raw .\data\transactions_raw_20251028.csv
  python upload_to_s3.py --bucket hapssatou-datalake-123 --region eu-west-3 --clean .\data\transactions_clean_local.csv --date 2025-10-28

Notes:
- Requires: pip install boto3
- Credentials must be configured via AWS CLI (aws configure) or env vars.
"""
import argparse
import os
import re
from datetime import datetime
import boto3

def infer_date_from_filename(path):
    # looks for 8 digits like 20251028 in filename; returns YYYY-MM-DD
    m = re.search(r'(20\d{6})', os.path.basename(path))
    if m:
        s = m.group(1)
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return None

def key_for(prefix, date_str, clean=False):
    d = datetime.strptime(date_str, "%Y-%m-%d")
    if clean:
        return f"transactions/clean/{d:%Y/%m/%d}/transactions_clean_{d:%Y%m%d}.csv"
    else:
        return f"transactions/raw/{d:%Y/%m/%d}/transactions_{d:%Y%m%d}.csv"

def upload_file(s3, bucket, src, key):
    s3.upload_file(src, bucket, key)
    print(f"Uploaded: {src}  ->  s3://{bucket}/{key}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True, help="S3 bucket name")
    ap.add_argument("--region", default="eu-west-3")
    ap.add_argument("--raw", help="Path to raw CSV file")
    ap.add_argument("--clean", help="Path to cleaned CSV file")
    ap.add_argument("--date", help="YYYY-MM-DD (optional; inferred from filename if omitted)")
    args = ap.parse_args()

    if not args.raw and not args.clean:
        ap.error("Provide at least --raw or --clean")

    session = boto3.session.Session(region_name=args.region)
    s3 = session.client("s3")

    if args.raw:
        date_str = args.date or infer_date_from_filename(args.raw) or datetime.utcnow().strftime("%Y-%m-%d")
        k = key_for("raw", date_str, clean=False)
        upload_file(s3, args.bucket, args.raw, k)

    if args.clean:
        date_str = args.date or infer_date_from_filename(args.clean) or datetime.utcnow().strftime("%Y-%m-%d")
        k = key_for("clean", date_str, clean=True)
        upload_file(s3, args.bucket, args.clean, k)

if __name__ == "__main__":
    main()