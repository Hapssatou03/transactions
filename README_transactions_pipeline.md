# Transactions Data Pipeline (Synthetic Dataset)

## Files
- Raw CSV: `transactions_raw_20251028.csv`
- Sample (first 100 rows): `transactions_sample_20251028.csv`
- Airflow DAG template: `airflow_dag_transactions.py`
- ETL script (cleaning): `etl_transform.py`

## Suggested S3 Layout
```
s3://hapssatou-datalake/
  └── transactions/
      ├── raw/      YYYY/MM/DD/transactions_YYYYMMDD.csv
      └── clean/    YYYY/MM/DD/transactions_clean_YYYYMMDD.csv
```

## Athena DDL (CSV in S3, OpenCSVSerde)
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS analytics.transactions_clean (
  transaction_id string,
  account_id string,
  transaction_ts timestamp,
  type string,
  amount double,
  currency string,
  category string,
  merchant string,
  payment_method string,
  status string,
  city string,
  country string,
  ingested_at string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
LOCATION 's3://hapssatou-datalake/transactions/clean/'
TBLPROPERTIES ('has_encrypted_data'='false');
```

## Example Athena Queries
```sql
-- 1) Dépenses vs Revenus par mois
SELECT date_trunc('month', transaction_ts) AS month,
       SUM(CASE WHEN type='DEBIT' THEN amount ELSE 0 END) AS total_debits,
       SUM(CASE WHEN type='CREDIT' THEN amount ELSE 0 END) AS total_credits
FROM analytics.transactions_clean
GROUP BY 1
ORDER BY 1;

-- 2) Top 10 marchands par dépenses
SELECT merchant,
       ABS(SUM(CASE WHEN type='DEBIT' THEN amount ELSE 0 END)) AS spend
FROM analytics.transactions_clean
GROUP BY merchant
ORDER BY spend DESC
LIMIT 10;

-- 3) Dépense moyenne par catégorie
SELECT category,
       ABS(AVG(CASE WHEN type='DEBIT' THEN amount END)) AS avg_spend
FROM analytics.transactions_clean
GROUP BY category
ORDER BY avg_spend DESC;

-- 4) Part des transactions échouées
SELECT status, COUNT(*) AS cnt
FROM analytics.transactions_clean
GROUP BY status;
```

## Airflow Notes
- Replace the placeholder S3 upload functions with real S3 operators (S3Hook / boto3) using an Airflow Connection (AWS).
- Set RAW_PATH and CLEAN_PATH via env vars or Variables in your Airflow.

## How to run cleaning locally
```bash
python etl_transform.py --input transactions_raw_20251028.csv --output transactions_clean_local.csv
```
