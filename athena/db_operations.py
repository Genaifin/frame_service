from typing import List, Dict, Optional
import os
import json
from pathlib import Path
from sqlalchemy import create_engine, text, inspect
from .query_cache import get_query_cache


def _pg_connection_string() -> str:
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def _get_engine():
    return create_engine(_pg_connection_string(), pool_pre_ping=True)


def _load_table_catalog() -> Dict:
    """Load the table catalog JSON file"""
    try:
        catalog_path = Path(__file__).parent / "table_catalog.json"
        with open(catalog_path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading table catalog: {str(e)}")
        return {"tables": {}, "global_query_patterns": {}}


def execute_query(query: str, use_cache: bool = True) -> List[Dict]:
    """Execute a SQL query and return results with optional caching"""
    try:
        use_cache = False
        # Check cache first if enabled
        if use_cache:
            cache = get_query_cache()
            cached_results = cache.get(query)
            if cached_results is not None:
                print("Query result retrieved from cache")
                return cached_results
        
        # Execute query
        engine = _get_engine()
        with engine.connect() as conn:
            try:
                conn.execute(text("SET search_path TO nexbridge, public"))
            except Exception:
                pass
            result = conn.execute(text(query))
            rows = [dict(row._mapping) for row in result]
            
            # Cache successful results if enabled
            if use_cache and isinstance(rows, list):
                cache.set(query, rows)
            
            return rows
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return {"error": str(e)}


def get_schema_info() -> Dict:
    """
    Get comprehensive schema information including columns, relationships, 
    and query patterns from both database and catalog
    """
    try:
        engine = _get_engine()
        insp = inspect(engine)
        schema_info: Dict[str, Dict] = {}
        catalog = _load_table_catalog()

        # Only whitelist important tables by schema
        target_tables: List[str] = []
        try:
            nexbridge_tables = insp.get_table_names(schema="nexbridge")
            public_tables = insp.get_table_names(schema="public")
        except Exception:
            nexbridge_tables = []
            public_tables = []

        # Expected important tables in nexbridge schema
        important_nexbridge = {
            "nav_pack",
            "navpack_version",
            "portfolio_valuation",
            "dividend",
            "trial_balance",
            "kpi_library",
            "kpi_thresholds",
            "source",
        }

        for t in nexbridge_tables:
            if t in important_nexbridge:
                target_tables.append(f"nexbridge.{t}")

        # Only benchmarks from public
        if "benchmarks" in public_tables:
            target_tables.append("public.benchmarks")

        for qualified in target_tables:
            try:
                schema, table = qualified.split(".", 1)
                
                # Get column info from database
                columns = []
                for col in insp.get_columns(table, schema=schema):
                    col_info = {
                        "name": col.get("name"),
                        "type": str(col.get("type")),
                        "nullable": col.get("nullable", True),
                    }
                    columns.append(col_info)
                
                # Get catalog info for this table
                catalog_table_info = catalog.get("tables", {}).get(qualified, {})
                
                # Build comprehensive schema info
                table_info = {
                    "columns": columns,
                    "description": catalog_table_info.get("description", ""),
                    "synonyms": catalog_table_info.get("synonyms", []),
                    "date_columns": catalog_table_info.get("date_columns", []),
                    "date_relationship_info": catalog_table_info.get("date_relationship_info", ""),
                    "relationships": catalog_table_info.get("relationships", []),
                    "common_queries": catalog_table_info.get("common_queries", []),
                    "query_patterns": catalog_table_info.get("query_patterns", {})
                }
                
                schema_info[qualified] = table_info
            except Exception as e:
                print(f"Error processing table {qualified}: {str(e)}")
                continue
        
        # Add global query patterns
        schema_info["_global_patterns"] = catalog.get("global_query_patterns", {})
        
        return schema_info
    except Exception as e:
        print(f"Error getting schema info: {str(e)}")
        return {}


def get_column_distinct_values(qualified_table: str, column_name: str, limit: int = 50) -> List[str]:
    """Get distinct values for a column to help LLM choose correct values"""
    try:
        engine = _get_engine()
        with engine.connect() as conn:
            try:
                conn.execute(text("SET search_path TO nexbridge, public"))
            except Exception:
                pass
            
            query = f"SELECT DISTINCT {column_name} FROM {qualified_table} WHERE {column_name} IS NOT NULL LIMIT {limit}"
            result = conn.execute(text(query))
            return [str(row[0]) for row in result]
    except Exception as e:
        print(f"Error getting distinct values for {qualified_table}.{column_name}: {str(e)}")
        return []


def get_enhanced_schema_info() -> Dict:
    """Get schema info enhanced with actual data statistics and distinct values"""
    schema_info = get_schema_info()
    
    # For critical enum columns, add actual distinct values from database
    critical_columns = {
        "nexbridge.trial_balance": ["type", "category"],
        "nexbridge.portfolio_valuation": ["inv_type"]
    }
    
    for table, columns in critical_columns.items():
        if table in schema_info:
            schema_info[table]["live_distinct_values"] = {}
            for col in columns:
                values = get_column_distinct_values(table, col, limit=20)
                if values:
                    schema_info[table]["live_distinct_values"][col] = values
                    print(f"Loaded {len(values)} distinct values for {table}.{col}")
    
    return schema_info


def get_latest_dates() -> str:
    """Get latest available dates across tables with better formatting"""
    try:
        engine = _get_engine()
        insp = inspect(engine)
        latest_date_info = "Latest available dates across tables:\n\n"
        with engine.connect() as conn:
            try:
                conn.execute(text("SET search_path TO nexbridge, public"))
            except Exception:
                pass

            # Build list of qualified tables like in get_schema_info
            try:
                nexbridge_tables = insp.get_table_names(schema="nexbridge")
                public_tables = insp.get_table_names(schema="public")
            except Exception:
                nexbridge_tables = []
                public_tables = []

            important_nexbridge = {
                "nav_pack",
                "navpack_version",
                "portfolio_valuation",
                "dividend",
                "trial_balance",
                "kpi_library",
                "kpi_thresholds",
                "source",
            }
            qualified_tables: List[str] = []
            for t in nexbridge_tables:
                if t in important_nexbridge:
                    qualified_tables.append(f"nexbridge.{t}")
            if "benchmarks" in public_tables:
                qualified_tables.append("public.benchmarks")

            # Special handling for nav_pack - this is the master date table
            try:
                res = conn.execute(text("SELECT MAX(file_date) AS max_date, MIN(file_date) AS min_date, COUNT(DISTINCT file_date) AS date_count FROM nexbridge.nav_pack"))
                row = res.fetchone()
                if row and row[0]:
                    latest_date_info += f"PRIMARY DATE SOURCE (nexbridge.nav_pack):\n"
                    latest_date_info += f"  Latest month: {row[0]}\n"
                    latest_date_info += f"  Earliest month: {row[1]}\n"
                    latest_date_info += f"  Total months available: {row[2]}\n\n"
            except Exception:
                pass

            # Get all available months
            try:
                res = conn.execute(text("SELECT file_date FROM nexbridge.nav_pack ORDER BY file_date DESC"))
                dates = [str(row[0]) for row in res]
                if dates:
                    latest_date_info += f"Available months: {', '.join(dates)}\n\n"
            except Exception:
                pass

            latest_date_info += "Other date columns:\n"
            for qualified in qualified_tables:
                if qualified == "nexbridge.nav_pack":
                    continue  # Already handled
                try:
                    schema, table = qualified.split(".", 1)
                    for col in insp.get_columns(table, schema=schema):
                        col_name = col.get("name", "")
                        if any(t in col_name.lower() for t in ["date", "time", "month", "year"]):
                            try:
                                res = conn.execute(text(f"SELECT MAX({col_name}) AS max_val FROM {qualified}"))
                                row = res.fetchone()
                                max_val = row and row[0]
                                if max_val is not None:
                                    latest_date_info += f"  {qualified}.{col_name}: {max_val}\n"
                            except Exception:
                                continue
                except Exception:
                    continue
        
        return latest_date_info
    except Exception as e:
        print(f"Error getting latest dates: {str(e)}")
        return "Error retrieving dates"


def get_source_id_from_name(fund_name: str) -> Optional[int]:
    """
    Get fund_id from fund name (case-insensitive matching)
    
    Args:
        fund_name: Fund name or ID from frontend (e.g., "NexBridge", "1", "nexbridge")
        
    Returns:
        fund_id if found, None otherwise
    """
    if not fund_name:
        return None
    
    try:
        engine = _get_engine()
        with engine.connect() as conn:
            try:
                conn.execute(text("SET search_path TO nexbridge, public"))
            except Exception:
                pass
            
            # First, try to get source.id from source table by name
            try:
                source_name = fund_name.strip()
                # Get source.id from source table where name matches (all possible LIKE combinations)
                query = text("""
                    SELECT id 
                    FROM nexbridge.source 
                    WHERE name = :source_name
                       OR name ILIKE :source_name
                       OR name LIKE :exact_match
                       OR name ILIKE :starts_with
                       OR name ILIKE :ends_with
                       OR name ILIKE :contains
                       OR LOWER(name) = LOWER(:source_name)
                    LIMIT 1
                """)
                result = conn.execute(query, {
                    "source_name": source_name,
                    "exact_match": source_name,
                    "starts_with": f"{source_name}%",
                    "ends_with": f"%{source_name}",
                    "contains": f"%{source_name}%"
                })
                row = result.fetchone()
                
                if row:
                    print(f"Found source by name: {row[0]}")
                    return row[0]
            except Exception as e:
                # Error in source lookup, continue to other matching methods
                print(f"Error looking up source by name: {str(e)}")
                pass
            
    except Exception as e:
        print(f"Error getting fund_id from name: {str(e)}")
        return None


def get_available_funds() -> List[Dict]:
    """
    Get list of all available funds in the database
    
    Returns:
        List of fund dictionaries with id, name, code, and data availability
    """
    try:
        engine = _get_engine()
        with engine.connect() as conn:
            try:
                conn.execute(text("SET search_path TO nexbridge, public"))
            except Exception:
                pass
            
            # Get funds with data from nav_pack
            query = text("""
                SELECT DISTINCT
                    np.fund_id,
                    f.name,
                    f.code,
                    MIN(np.file_date) as earliest_date,
                    MAX(np.file_date) as latest_date,
                    COUNT(DISTINCT np.file_date) as month_count
                FROM nexbridge.nav_pack np
                LEFT JOIN public.funds f ON np.fund_id = f.id
                GROUP BY np.fund_id, f.name, f.code
                ORDER BY np.fund_id
            """)
            result = conn.execute(query)
            funds = []
            
            for row in result:
                fund_id = row[0]
                funds.append({
                    "fund_id": fund_id,
                    "name": row[1] if row[1] else f"Fund {fund_id}",
                    "code": row[2] if row[2] else str(fund_id),
                    "earliest_date": str(row[3]) if row[3] else None,
                    "latest_date": str(row[4]) if row[4] else None,
                    "month_count": row[5] if row[5] else 0
                })
            
            return funds
            
    except Exception as e:
        print(f"Error getting available funds: {str(e)}")
        import traceback
        traceback.print_exc()
        return []