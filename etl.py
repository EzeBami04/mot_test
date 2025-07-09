import os
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
BASE_URL = "https://data.gov.il/api/3/action/datastore_search"

def generate_table_schema(df: pd.DataFrame, table_name: str) -> str:
    type_mapping = {
        'int64': 'INTEGER', 'float64': 'FLOAT',
        'object': 'TEXT', 'string': 'TEXT',
        'bool': 'BOOLEAN', 'Int64': 'INTEGER',
        'datetime64[ns]': 'TIMESTAMP'
    }
    columns = []
    for col in df.columns:
        clean_col = col.replace(' ', '_').replace(':', '_').replace('-', '_')
        dtype = str(df[col].dtype)
        if clean_col == '_id':
            columns.append("_id INTEGER PRIMARY KEY")
        else:
            columns.append(f'"{clean_col}" {type_mapping.get(dtype, "TEXT")}')
    return f'CREATE TABLE IF NOT EXISTS public.{table_name} ({", ".join(columns)});'

# Extractor
def extractor(resource_id: str) -> pd.DataFrame:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    all_records = []

    try:
        initial_res = session.get(BASE_URL, params={"resource_id": resource_id, "limit": 1})
        initial_res.raise_for_status()
        total = initial_res.json()['result']['total']
        logging.info(f"Fetching {total} records for resource {resource_id}")

        for offset in range(0, total, 50000):
            res = session.get(BASE_URL, params={"resource_id": resource_id, "limit": 50000, "offset": offset})
            if res.status_code == 200:
                batch = res.json()['result']['records']
                all_records.extend(batch)
                logging.info(f"Fetched {len(batch)} at offset {offset}")
            else:
                logging.error(f"Failed at offset {offset} with HTTP {res.status_code}")
                break

        df = pd.DataFrame(all_records)
        df.columns = [col.lower() for col in df.columns]
        logging.info(f"Extracted {len(df)} total records for {resource_id}")
        return df
    except Exception as e:
        logging.error(f"Extraction failed for {resource_id}: {e}")
        return pd.DataFrame()

# Monitoring
def create_monitoring_table(conn_str: str):
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.etl_monitoring (
                    id SERIAL PRIMARY KEY,
                    resource_id TEXT,
                    table_name TEXT,
                    load_time TIMESTAMP,
                    row_count INTEGER,
                    notes TEXT
                );
            """)
            conn.commit()
            logging.info("Monitoring table 'etl_monitoring' ensured in database.")

def log_monitoring_entry(conn_str: str, resource_id: str, table_name: str, row_count: int, status: str):
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO public.etl_monitoring (resource_id, table_name, load_time, row_count, notess)
                VALUES (%s, %s, %s, %s, %s);
            """, (resource_id, table_name, datetime.now(), row_count, status))
            conn.commit()
            logging.info(f"Logged monitoring entry for {table_name} with status '{status}'.")

# Load
def load_to_table(resource_id: str, table_name: str, conn_str: str):
    df = extractor(resource_id)
    if df.empty:
        logging.warning(f"No data loaded for {table_name}")
        log_monitoring_entry(conn_str, resource_id, table_name, 0, "No data")
        return

    # Clean
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.lower()
        df[col].replace({pd.NA: None, float('nan'): None}, inplace=True)

    if '_id' in df.columns:
        df['_id'] = pd.to_numeric(df['_id'], errors='coerce').astype('Int64')
        if df['_id'].isna().any():
            logging.error(f"NaN in _id for {table_name}")
            log_monitoring_entry(conn_str, resource_id, table_name, len(df), "NaN in _id")
            return
        if not df['_id'].is_unique:
            logging.error(f"Duplicate _id in {table_name}")
            log_monitoring_entry(conn_str, resource_id, table_name, len(df), "Duplicate _id")
            return

    numeric_columns = [c for c in df.columns if c.startswith('kamut_') or c.endswith('_WLTP') or c in ('nikud_betihut', 'CO2_WLTP_NEDC')]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Load
    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SET search_path TO public;")
                create_sql = generate_table_schema(df, table_name)
                cursor.execute(f"DROP TABLE IF EXISTS public.{table_name};")
                cursor.execute(create_sql)
                logging.info(f"Created table {table_name}")

                engine = create_engine(conn_str)
                staging_table = f"{table_name}_staging"
                df.to_sql(staging_table, con=engine, index=False, if_exists='replace', schema='public')
                logging.info(f"Loaded data into staging {staging_table} with {len(df)} rows.")

                upsert_cols = [f'"{c}"' for c in df.columns]
                update_stmt = ", ".join(f"{col}=EXCLUDED.{col}" for col in upsert_cols if col != '"_id"')

                upsert_sql = f"""
                    INSERT INTO public.{table_name} ({", ".join(upsert_cols)})
                    SELECT {", ".join(upsert_cols)} FROM public.{staging_table}
                    ON CONFLICT (_id) DO UPDATE SET {update_stmt};
                """
                cursor.execute(upsert_sql)
                logging.info(f"Upserted data into {table_name}")

                cursor.execute(f"DROP TABLE IF EXISTS public.{staging_table};")
                logging.info(f"Dropped staging table {staging_table}")
            conn.commit()
        log_monitoring_entry(conn_str, resource_id, table_name, len(df), "Success")
    except Exception as e:
        logging.error(f"Load failed for {table_name}: {e}")
        log_monitoring_entry(conn_str, resource_id, table_name, len(df), "Failed")

