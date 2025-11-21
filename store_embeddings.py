import os
import sys
import json
from pathlib import Path
from typing import Dict, List, Optional
 
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores.pgvector import PGVector
 
 
# Load environment variables
load_dotenv(override=True)
 
 
# ============================================================================
# CONFIGURATION
# ============================================================================
 
# Vector collection name (change if you want different collection)
COLLECTION_NAME = "aithon_fund_data"
 
# Representative example rows per month
EXAMPLE_ROWS_PER_MONTH = 5
 
# Sample limit for testing (set to None for production - loads all data)
SAMPLE_LIMIT = None  # Change to 100 for quick testing
 
 
# ============================================================================
# DATABASE CONNECTION
# ============================================================================
 
def get_connection_string() -> str:
    """Build PostgreSQL connection string from environment variables"""
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    
    if not all([host, port, db, user, password]):
        raise ValueError(
            "Missing database credentials. Please set: "
            "DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD"
        )
    
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
 
 
def get_engine():
    """Create SQLAlchemy engine"""
    return create_engine(get_connection_string(), pool_pre_ping=True)
 
 
# ============================================================================
# TABLE CATALOG LOADING
# ============================================================================
 
def load_table_catalog() -> Dict:
    """Load table catalog with metadata"""
    try:
        catalog_path = Path(__file__).parent / "athena" / "table_catalog.json"
        if not catalog_path.exists():
            print(f"WARNING: table_catalog.json not found at {catalog_path}")
            return {"tables": {}}
        
        with open(catalog_path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading table catalog: {str(e)}")
        return {"tables": {}}
 
 
def get_catalog_tables() -> List[str]:
    """Get list of tables from catalog"""
    catalog = load_table_catalog()
    tables = list(catalog.get("tables", {}).keys())
    # Filter out global patterns
    tables = [t for t in tables if not t.startswith("_")]
    return tables
 
 
# ============================================================================
# DATA LOADING
# ============================================================================
 
def load_tables_from_db(sample_limit: Optional[int] = None) -> Dict[str, pd.DataFrame]:
    """
    Load all important tables from the database
    
    Args:
        sample_limit: If set, limits rows per table (for testing)
    
    Returns:
        Dictionary mapping table names to DataFrames
    """
    engine = get_engine()
    tables = get_catalog_tables()
    fund_data: Dict[str, pd.DataFrame] = {}
    
    if not tables:
        print("ERROR: No tables found in catalog")
        return fund_data
 
    print(f"\nLoading {len(tables)} tables from database...")
    
    with engine.connect() as conn:
        try:
            conn.execute(text("SET search_path TO nexbridge, public"))
        except Exception:
            pass
 
        for qualified in tables:
            try:
                print(f"  Loading {qualified}...", end=" ")
                
                # For tables with navpack_version_id, join with nav_pack to get dates
                if qualified in ["nexbridge.trial_balance", "nexbridge.portfolio_valuation", "nexbridge.dividend"]:
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
                    print(f"✓ {len(df)} rows, {len(df.columns)} columns")
                else:
                    print("⚠ No data")
                    
            except Exception as e:
                print(f"✗ Error: {str(e)}")
                continue
    
    return fund_data
 
 
# ============================================================================
# EMBEDDING GENERATION
# ============================================================================
 
def create_table_summary(df: pd.DataFrame, qualified_table: str, catalog: Dict) -> str:
    """Create a summary document for a table"""
    table_info = catalog.get("tables", {}).get(qualified_table, {})
    schema, table = qualified_table.split(".", 1)
    
    rows_count = len(df)
    columns_count = len(df.columns)
    columns_list = ", ".join(df.columns.tolist()[:20])
    date_columns = ", ".join(table_info.get("date_columns", []))
    
    desc = table_info.get("description", "")
    synonyms = ", ".join(table_info.get("synonyms", [])[:10])
    
    summary = [
        f"Table: {qualified_table}",
        f"Schema: {schema}",
        f"Description: {desc}",
        f"Synonyms: {synonyms}",
        f"Rows: {rows_count}, Columns: {columns_count}",
        f"Columns: {columns_list}",
        f"Date columns: {date_columns}",
    ]
    
    return "\n".join([s for s in summary if s])
 
 
def create_monthly_summary(df: pd.DataFrame, qualified_table: str, month_col: str) -> List[tuple]:
    """Create monthly summary documents"""
    documents = []
    try:
        df = df.copy()
        df["__month__"] = df[month_col].dt.to_period("M").dt.to_timestamp()
        numeric_columns = df.select_dtypes(include=["number"]).columns.tolist()
        month_groups = df.groupby("__month__", dropna=True)
        
        for month_value, gdf in month_groups:
            month_label = pd.to_datetime(month_value).strftime("%Y-%m")
            parts = [
                f"Monthly summary for {qualified_table} @ {month_label}",
                f"Total rows: {len(gdf)}"
            ]
            
            # Add categorical breakdowns
            if "type" in gdf.columns:
                type_counts = gdf["type"].value_counts()
                parts.append(f"Breakdown by type: {type_counts.to_dict()}")
            
            if "inv_type" in gdf.columns:
                inv_type_counts = gdf["inv_type"].value_counts()
                parts.append(f"Investment types: {inv_type_counts.to_dict()}")
            
            # Extract vendor names from JSON if available
            if qualified_table == "nexbridge.trial_balance" and "extra_data" in gdf.columns:
                try:
                    vendors = set()
                    for extra_data in gdf["extra_data"].dropna():
                        if extra_data and extra_data.strip():
                            data = json.loads(extra_data)
                            if "general_ledger" in data and isinstance(data["general_ledger"], list):
                                for entry in data["general_ledger"]:
                                    if "tran_description" in entry:
                                        vendors.add(entry["tran_description"])
                    if vendors:
                        parts.append(f"Vendors/Law Firms in transactions: {', '.join(sorted(list(vendors)[:10]))}")
                        if len(vendors) > 10:
                            parts.append(f"  (and {len(vendors) - 10} more vendors)")
                except Exception:
                    pass
            
            if numeric_columns:
                totals = gdf[numeric_columns].sum(numeric_only=True).sort_values(ascending=False)
                top_totals = totals.head(8)
                parts.append("Key metrics:")
                for col, val in top_totals.items():
                    try:
                        parts.append(f"  {col}: {float(val):,.2f}")
                    except Exception:
                        parts.append(f"  {col}: {val}")
            
            summary_text = "\n".join(parts)
            documents.append((month_label, summary_text))
    except Exception as e:
        print(f"    Warning: Could not create monthly summary: {str(e)}")
    
    return documents
 
 
def select_representative_rows(df: pd.DataFrame, k: int) -> pd.DataFrame:
    """Select representative example rows"""
    if df.empty:
        return df
    numeric = df.select_dtypes(include=["number"]).copy()
    if numeric.empty:
        return df.head(k)
    try:
        score = numeric.abs().sum(axis=1)
        top_idx = score.nlargest(k).index
        return df.loc[top_idx]
    except Exception:
        return df.head(k)
 
 
def store_embeddings(fund_data: Dict[str, pd.DataFrame], force_overwrite: bool = True) -> bool:
    """
    Generate and store embeddings in pgvector
    
    Args:
        fund_data: Dictionary of table DataFrames
        force_overwrite: If True, deletes existing collection and creates new one
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Initialize OpenAI embeddings
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY not set in environment")
        
        print("\n" + "="*80)
        print("GENERATING EMBEDDINGS")
        print("="*80)
        
        embeddings = OpenAIEmbeddings(
            model="text-embedding-3-small",
            api_key=openai_api_key,
        )
        
        connection_string = get_connection_string()
        catalog = load_table_catalog()
        documents: List[Document] = []
        
        # Add query patterns from catalog
        print("\n1. Adding query patterns from catalog...")
        pattern_count = 0
        for qualified_table, table_info in catalog.get("tables", {}).items():
            if qualified_table.startswith("_"):
                continue
            
            query_patterns = table_info.get("query_patterns", {})
            for pattern_name, pattern_info in query_patterns.items():
                if isinstance(pattern_info, dict):
                    pattern_text = [
                        f"Query Pattern: {pattern_name}",
                        f"Table: {qualified_table}",
                        f"Description: {pattern_info.get('description', '')}",
                        f"When to use: {pattern_info.get('when_to_use', '')}",
                        f"Pattern: {pattern_info.get('pattern', '')}",
                        f"Example: {pattern_info.get('example', pattern_info.get('full_example', ''))[:500]}",
                    ]
                    documents.append(
                        Document(
                            page_content="\n".join([t for t in pattern_text if t]),
                            metadata={
                                "fund_name": "NexBridge",
                                "qualified_table": qualified_table,
                                "chunk_type": "query_pattern",
                                "pattern_name": pattern_name,
                                "keywords": pattern_info.get('description', ''),
                            },
                        )
                    )
                    pattern_count += 1
        print(f"   Added {pattern_count} query patterns")
        
        # Process each table
        print("\n2. Processing tables and creating embeddings...")
        for raw_table_name, df in fund_data.items():
            print(f"\n   Processing {raw_table_name}...")
            
            # Infer qualified name
            if "." in raw_table_name:
                schema, table = raw_table_name.split(".", 1)
                qualified = raw_table_name
            else:
                schema, table = "nexbridge", raw_table_name
                qualified = f"nexbridge.{raw_table_name}"
            
            table_info = catalog.get("tables", {}).get(qualified, {})
            synonyms = ", ".join(table_info.get("synonyms", [])[:10])
            
            # 1) Table summary
            table_summary = create_table_summary(df, qualified, catalog)
            documents.append(
                Document(
                    page_content=table_summary,
                    metadata={
                        "fund_name": "NexBridge",
                        "schema": schema,
                        "table": table,
                        "qualified_table": qualified,
                        "chunk_type": "table_summary",
                        "time_scope": "",
                        "keywords": synonyms,
                    },
                )
            )
            print(f"      ✓ Table summary created")
            
            # 2) Monthly summaries if date column exists
            date_columns = table_info.get("date_columns", [])
            month_col = None
            
            if "nav_date" in df.columns:
                month_col = "nav_date"
            else:
                for c in date_columns:
                    if c in df.columns:
                        month_col = c
                        break
            
            if month_col and month_col in df.columns:
                # Convert to datetime
                if not pd.api.types.is_datetime64_any_dtype(df[month_col]):
                    try:
                        df[month_col] = pd.to_datetime(df[month_col], errors='coerce')
                    except Exception:
                        month_col = None
            
            if month_col and pd.api.types.is_datetime64_any_dtype(df[month_col]):
                monthly_docs = create_monthly_summary(df, qualified, month_col)
                for month_label, summary_text in monthly_docs:
                    documents.append(
                        Document(
                            page_content=summary_text,
                            metadata={
                                "fund_name": "NexBridge",
                                "schema": schema,
                                "table": table,
                                "qualified_table": qualified,
                                "chunk_type": "monthly_summary",
                                "time_scope": month_label,
                                "keywords": synonyms,
                            },
                        )
                    )
                print(f"      ✓ {len(monthly_docs)} monthly summaries created")
                
                # 3) Representative example rows per month
                try:
                    df_copy = df.copy()
                    df_copy["__month__"] = df_copy[month_col].dt.to_period("M").dt.to_timestamp()
                    example_count = 0
                    for month_value, gdf in df_copy.groupby("__month__", dropna=True):
                        month_label = pd.to_datetime(month_value).strftime("%Y-%m")
                        top_df = select_representative_rows(gdf, EXAMPLE_ROWS_PER_MONTH)
                        top_df = top_df.drop(columns=["__month__"], errors="ignore")
                        if not top_df.empty:
                            preview = top_df.to_string(max_rows=EXAMPLE_ROWS_PER_MONTH)
                            header = f"Representative rows for {qualified} @ {month_label}"
                            documents.append(
                                Document(
                                    page_content=f"{header}\n{preview}",
                                    metadata={
                                        "fund_name": "NexBridge",
                                        "schema": schema,
                                        "table": table,
                                        "qualified_table": qualified,
                                        "chunk_type": "examples",
                                        "time_scope": month_label,
                                        "keywords": synonyms,
                                    },
                                )
                            )
                            example_count += 1
                    print(f"      ✓ {example_count} example row sets created")
                except Exception as e:
                    print(f"      ⚠ Could not create examples: {str(e)}")
        
        # Store in pgvector
        print("\n3. Storing embeddings in pgvector...")
        print(f"   Total documents to embed: {len(documents)}")
        print(f"   Collection name: {COLLECTION_NAME}")
        print(f"   Force overwrite: {force_overwrite}")
        print("\n   This may take several minutes...")
        
        PGVector.from_documents(
            documents=documents,
            embedding=embeddings,
            connection_string=connection_string,
            collection_name=COLLECTION_NAME,
            pre_delete_collection=bool(force_overwrite),
        )
        
        print("\n   ✓ Embeddings stored successfully!")
        return True
        
    except Exception as e:
        print(f"\n   ✗ Error storing embeddings: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
 
 
# ============================================================================
# MAIN EXECUTION
# ============================================================================
 
def check_prerequisites():
    """Check if all prerequisites are met"""
    print("\n" + "="*80)
    print("CHECKING PREREQUISITES")
    print("="*80)
    
    errors = []
    
    # Check environment variables
    required_env = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "OPENAI_API_KEY"]
    for var in required_env:
        if not os.getenv(var):
            errors.append(f"Missing environment variable: {var}")
        else:
            print(f"✓ {var}: Set")
    
    # Check table catalog exists
    catalog_path = Path(__file__).parent / "athena" / "table_catalog.json"
    if catalog_path.exists():
        print(f"✓ Table catalog: Found at {catalog_path}")
    else:
        errors.append(f"Table catalog not found at {catalog_path}")
    
    # Check database connection
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✓ Database connection: Success")
    except Exception as e:
        errors.append(f"Database connection failed: {str(e)}")
    
    # Check pgvector extension
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM pg_extension WHERE extname = 'vector'"))
            if result.fetchone():
                print("✓ pgvector extension: Installed")
            else:
                errors.append("pgvector extension not installed. Run: CREATE EXTENSION vector;")
    except Exception as e:
        errors.append(f"Could not check pgvector: {str(e)}")
    
    if errors:
        print("\n" + "="*80)
        print("ERRORS FOUND:")
        print("="*80)
        for error in errors:
            print(f"  ✗ {error}")
        print("\nPlease fix these errors before running the script.")
        return False
    
    print("\n✓ All prerequisites met!")
    return True
 
 
def main():
    """Main execution function"""
    print("\n" + "="*80)
    print("ATHENA CHATBOT - EMBEDDING STORAGE")
    print("="*80)
    print("\nThis script will:")
    print("1. Load data from PostgreSQL database")
    print("2. Generate embeddings using OpenAI")
    print("3. Store embeddings in pgvector for semantic search")
    print("\nEstimated time: 5-15 minutes")
    print("="*80)
    
    # Check prerequisites
    if not check_prerequisites():
        sys.exit(1)
    
    # Ask for confirmation
    print("\n" + "="*80)
    response = input("Proceed with embedding generation? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("Aborted.")
        sys.exit(0)
    
    try:
        # Step 1: Load data from database
        print("\n" + "="*80)
        print("STEP 1: LOADING DATA FROM DATABASE")
        print("="*80)
        
        data = load_tables_from_db(sample_limit=SAMPLE_LIMIT)
        
        if not data:
            print("\n✗ ERROR: No tables loaded from database")
            sys.exit(1)
        
        print(f"\n✓ Successfully loaded {len(data)} tables")
        total_rows = sum(len(df) for df in data.values())
        print(f"  Total rows: {total_rows:,}")
        
        # Step 2: Generate and store embeddings
        print("\n" + "="*80)
        print("STEP 2: GENERATING AND STORING EMBEDDINGS")
        print("="*80)
        
        success = store_embeddings(data, force_overwrite=True)
        
        if success:
            print("\n" + "="*80)
            print("SUCCESS!")
            print("="*80)
            print("\nEmbeddings have been successfully stored in the database.")
            print("The chatbot is now ready to use semantic search for answering questions.")
            
            # Verify embeddings were created
            print("\n" + "="*80)
            print("VERIFICATION")
            print("="*80)
            try:
                engine = get_engine()
                with engine.connect() as conn:
                    result = conn.execute(text(
                        f"SELECT COUNT(*) FROM langchain_pg_embedding WHERE collection_name = '{COLLECTION_NAME}'"
                    ))
                    count = result.fetchone()[0]
                    print(f"✓ Total embeddings created: {count}")
                    
                    result = conn.execute(text(
                        f"SELECT cmetadata->>'chunk_type' as type, COUNT(*) "
                        f"FROM langchain_pg_embedding "
                        f"WHERE collection_name = '{COLLECTION_NAME}' "
                        f"GROUP BY cmetadata->>'chunk_type'"
                    ))
                    print("\nBreakdown by type:")
                    for row in result:
                        print(f"  - {row[0]}: {row[1]} embeddings")
            except Exception as e:
                print(f"⚠ Could not verify embeddings: {str(e)}")
            
            print("\nNext steps:")
            print("1. Test the chatbot with sample questions")
            print("2. Deploy the updated system")
            print("3. Monitor chatbot performance")
        else:
            print("\n" + "="*80)
            print("FAILED")
            print("="*80)
            print("\nEmbedding storage failed. Please check the errors above.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ FATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
 
 
if __name__ == "__main__":
    main()
