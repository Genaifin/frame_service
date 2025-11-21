import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from .vector_store import store_fund_embeddings, _pg_connection_string


load_dotenv(override=True)


def _get_engine():
    return create_engine(_pg_connection_string(), pool_pre_ping=True)


def _load_catalog_tables() -> List[str]:
    """Load table list from catalog"""
    try:
        catalog_path = Path(__file__).parent / "table_catalog.json"
        data = json.loads(Path(catalog_path).read_text())
        tables = list((data or {}).get("tables", {}).keys())
        # Filter out global patterns
        tables = [t for t in tables if not t.startswith("_")]
        return tables
    except Exception as e:
        print(f"Error loading catalog: {str(e)}")
        return []


def load_tables_from_db(sample_limit: Optional[int] = None) -> Dict[str, pd.DataFrame]:
    """
    Load all important tables from the database
    """
    engine = _get_engine()
    tables = _load_catalog_tables()
    fund_data: Dict[str, pd.DataFrame] = {}
    
    if not tables:
        print("No tables found in catalog")
        return fund_data

    with engine.connect() as conn:
        try:
            conn.execute(text("SET search_path TO nexbridge, public"))
        except Exception:
            pass

        for qualified in tables:
            try:
                print(f"Loading table: {qualified}")
                
                # For tables with navpack_version_id, join with nav_pack to get dates
                if qualified in ["nexbridge.trial_balance", "nexbridge.portfolio_valuation", "nexbridge.dividend"]:
                    # Load with date information joined
                    query = f"""
                    SELECT 
                        dt.*,
                        np.file_date as nav_date
                    FROM {qualified} dt
                    JOIN nexbridge.navpack_version nv ON dt.navpack_version_id = nv.navpack_version_id
                    JOIN nexbridge.nav_pack np ON nv.navpack_id = np.navpack_id
                    """
                    if sample_limit and int(sample_limit) > 0:
                        query += f" LIMIT {int(sample_limit)}"
                    
                    df = pd.read_sql_query(query, conn)
                else:
                    # Regular table load
                    limit_clause = f" LIMIT {int(sample_limit)}" if sample_limit and int(sample_limit) > 0 else ""
                    df = pd.read_sql_query(f"SELECT * FROM {qualified}{limit_clause}", conn)
                
                if not df.empty:
                    fund_data[qualified] = df
                    print(f"  Loaded {len(df)} rows")
                else:
                    print(f"  No data in {qualified}")
            except Exception as e:
                print(f"  Error loading {qualified}: {str(e)}")
                continue
    
    return fund_data


def main() -> None:
    """
    Main function to embed data from database into vector store
    
    This will:
    1. Load all tables from the database
    2. Create comprehensive embeddings with enhanced strategy
    3. Store in pgvector for semantic search
    """
    print("="*80)
    print("ATHENA: Embedding Database Data into Vector Store")
    print("="*80)
    
    # Load all data (no sample limit for production)
    # Use sample_limit=100 for testing
    print("\n[1/3] Loading data from database...")
    data = load_tables_from_db(sample_limit=None)
    
    if not data:
        print("ERROR: No tables loaded from database for embeddings")
        return
    
    print(f"\n[2/3] Loaded {len(data)} tables:")
    for table_name, df in data.items():
        print(f"  - {table_name}: {len(df)} rows, {len(df.columns)} columns")
    
    print("\n[3/3] Creating and storing embeddings...")
    print("This may take a few minutes depending on data size...")
    
    # Store embeddings with force_overwrite=True to refresh all data
    ok = store_fund_embeddings("NexBridge", data, force_overwrite=True)
    
    if ok:
        print("\n" + "="*80)
        print("SUCCESS: Embeddings stored successfully!")
        print("="*80)
        print("\nThe system is now ready to answer questions about the data.")
        print("Vector search will provide relevant context for SQL query generation.")
    else:
        print("\n" + "="*80)
        print("FAILED: Could not store embeddings")
        print("="*80)
        print("\nPlease check:")
        print("  1. PostgreSQL is running and accessible")
        print("  2. pgvector extension is installed")
        print("  3. OPENAI_API_KEY is set correctly in environment")
        print("  4. Network connectivity to OpenAI API")


if __name__ == "__main__":
    main()