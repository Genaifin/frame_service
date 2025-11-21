import os
from pathlib import Path
from typing import Dict
import pandas as pd
from sqlalchemy import create_engine, text

from .vector_store import store_fund_embeddings

DATA_DIR = Path("data/validusDemo/l1")


def _pg_connection_string() -> str:
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def _get_engine():
    return create_engine(_pg_connection_string(), pool_pre_ping=True)


def get_data() -> Dict[str, pd.DataFrame]:
    data: Dict[str, pd.DataFrame] = {}
    # Use table names derived directly from CSV filenames (without extension)
    files = [
        # "positions_with_fx.csv",
        # "NAV_Validations.csv",
    ]
    for file_name in files:
        file_path = DATA_DIR / file_name
        if file_path.exists():
            df = pd.read_csv(file_path)
            # Normalize table name to Postgres-friendly lower-case
            table_name = Path(file_name).stem.lower()
            data[table_name] = df
    return data


def save_to_db(data: Dict[str, pd.DataFrame]) -> bool:
    try:
        engine = _get_engine()
        with engine.begin() as conn:
            for table_name, df in data.items():
                # normalize column names to lowercase snake-case for Postgres
                df = df.copy()
                df.columns = [str(c).strip().replace(' ', '_').lower() for c in df.columns]
                # parse common date columns into pandas datetime so pandas writes DATE/TIMESTAMP
                for col in list(df.columns):
                    if any(t in col for t in ["date", "time", "month", "year"]):
                        try:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        except Exception:
                            pass
                # Replace table atomically
                tmp_table = f"{table_name}__staging"
                df.to_sql(tmp_table, conn, if_exists="replace", index=False)
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.execute(text(f"ALTER TABLE {tmp_table} RENAME TO {table_name}"))
        return True
    except Exception as e:
        print(f"Error saving to Postgres: {str(e)}")
        return False


def process_data() -> bool:
    try:
        data = get_data()
        if not data:
            print("No source CSVs found to process")
            return False
        if not save_to_db(data):
            return False

        # Vectorize into pgvector, single fund context retained in metadata
        # If embeddings fail (e.g., missing GEMINI_API_KEY), continue; SQL answering can still work using schema
        if not store_fund_embeddings("NexBridge", data, force_overwrite=False):
            print("Warning: Skipped vector embeddings (pgvector). Proceeding with tables only.")
        return True
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        return False