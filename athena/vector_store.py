from typing import Dict, Optional, List, Tuple
import os
import json
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores.pgvector import PGVector

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    raise ValueError(
        "OPENAI_API_KEY environment variable is not set. Please check your .env file."
    )

# Postgres connection (pgvector)
PG_HOST = os.getenv("DB_HOST")
PG_PORT = os.getenv("DB_PORT")
PG_DB = os.getenv("DB_NAME")
PG_USER = os.getenv("DB_USER")
PG_PASSWORD = os.getenv("DB_PASSWORD")

# Vector collection name
COLLECTION_NAME = "aithon_fund_data"

# Representative example rows per month (not per-row embeddings)
EXAMPLE_ROWS_PER_MONTH = 5


def _pg_connection_string() -> str:
    user = PG_USER
    password = PG_PASSWORD
    host = PG_HOST
    port = PG_PORT
    db = PG_DB
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def _unused_removed_marker() -> None:
    return None

_CATALOG_CACHE: Optional[Dict] = None


def _load_table_catalog() -> Dict:
    global _CATALOG_CACHE
    if _CATALOG_CACHE is not None:
        return _CATALOG_CACHE
    try:
        catalog_path = Path(__file__).parent / "table_catalog.json"
        with open(catalog_path, "r") as f:
            _CATALOG_CACHE = json.load(f)
            return _CATALOG_CACHE
    except Exception:
        _CATALOG_CACHE = {"tables": {}}
        return _CATALOG_CACHE


def _infer_qualified_table_name(table_name: str) -> Tuple[str, str, str]:
    """
    Returns (schema, table, qualified) using the table catalog when available.
    Accepts either bare table name or schema-qualified name.
    """
    catalog = _load_table_catalog().get("tables", {})
    if "." in table_name:
        schema, table = table_name.split(".", 1)
        qualified = f"{schema}.{table}"
        return schema, table, qualified
    # try nexbridge first
    candidate = f"nexbridge.{table_name}"
    if candidate in catalog:
        return "nexbridge", table_name, candidate
    candidate = f"public.{table_name}"
    if candidate in catalog:
        return "public", table_name, candidate
    # default to nexbridge if unknown
    return "nexbridge", table_name, f"nexbridge.{table_name}"


def _get_table_meta(qualified_table: str) -> Dict:
    return _load_table_catalog().get("tables", {}).get(qualified_table, {})


def _get_date_columns_for_table(qualified_table: str) -> List[str]:
    meta = _get_table_meta(qualified_table)
    return list(meta.get("date_columns", []))


def _coerce_datetime_columns(df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
    if not date_columns:
        return df
    df = df.copy()
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass
    return df


def _format_month(dt_val) -> Optional[str]:
    try:
        if pd.isna(dt_val):
            return None
        return pd.to_datetime(dt_val).strftime("%Y-%m")
    except Exception:
        return None


def _create_table_summary(df: pd.DataFrame, qualified_table: str) -> str:
    meta = _get_table_meta(qualified_table)
    schema, table = qualified_table.split(".", 1)
    rows_count = len(df)
    columns_count = len(df.columns)
    columns_list = ", ".join(df.columns.tolist()[:20])
    date_columns = ", ".join(_get_date_columns_for_table(qualified_table))
    desc = meta.get("description", "")
    synonyms = ", ".join(meta.get("synonyms", [])[:10])
    text = [
        f"Table: {qualified_table}",
        f"Schema: {schema}",
        f"Description: {desc}",
        f"Synonyms: {synonyms}",
        f"Rows: {rows_count}, Columns: {columns_count}",
        f"Columns: {columns_list}",
        f"Date columns: {date_columns}",
    ]
    return "\n".join([t for t in text if t])

def _build_monthly_summary(df: pd.DataFrame, qualified_table: str, month_col: str) -> List[Tuple[str, str]]:
    """
    Returns list of (month_label, summary_text)
    Enhanced to include more detailed summaries for financial data
    """
    documents: List[Tuple[str, str]] = []
    try:
        df = df.copy()
        df["__month__"] = df[month_col].dt.to_period("M").dt.to_timestamp()
        numeric_columns = df.select_dtypes(include=["number"]).columns.tolist()
        month_groups = df.groupby("__month__", dropna=True)
        
        for month_value, gdf in month_groups:
            month_label = _format_month(month_value)
            parts: List[str] = []
            parts.append(f"Monthly summary for {qualified_table} @ {month_label}")
            parts.append(f"Total rows: {len(gdf)}")
            
            # Add categorical breakdown if available
            if "type" in gdf.columns:
                type_counts = gdf["type"].value_counts()
                parts.append(f"Breakdown by type: {type_counts.to_dict()}")
            
            if "inv_type" in gdf.columns:
                inv_type_counts = gdf["inv_type"].value_counts()
                parts.append(f"Investment types: {inv_type_counts.to_dict()}")
            
            # Extract vendor/law firm names from extra_data JSON if it exists
            if qualified_table == "nexbridge.trial_balance" and "extra_data" in gdf.columns:
                try:
                    vendors = set()
                    for extra_data in gdf["extra_data"].dropna():
                        if extra_data and extra_data.strip():
                            import json as json_lib
                            data = json_lib.loads(extra_data)
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
                # Enhanced numeric summaries
                totals = gdf[numeric_columns].sum(numeric_only=True).sort_values(ascending=False)
                top_totals = totals.head(8)  # Increased from 5 to 8
                parts.append("Key metrics:")
                for col, val in top_totals.items():
                    try:
                        parts.append(f"  {col}: {float(val):,.2f}")
                    except Exception:
                        parts.append(f"  {col}: {val}")
            
            summary_text = "\n".join(parts)
            documents.append((month_label or "", summary_text))
    except Exception as e:
        print(f"Error building monthly summary: {str(e)}")
        return []
    return documents


def _select_representative_rows(df: pd.DataFrame, k: int) -> pd.DataFrame:
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
def store_fund_embeddings(fund_name: str, fund_data: Dict[str, pd.DataFrame], force_overwrite: bool = False) -> bool:
    try:
        # Build embedding function
        embeddings = OpenAIEmbeddings(
            model="text-embedding-3-small",
            api_key=OPENAI_API_KEY,
        )

        connection_string = _pg_connection_string()

        documents: List[Document] = []
        
        # Add query pattern documents from catalog
        catalog = _load_table_catalog()
        for qualified_table, table_info in catalog.get("tables", {}).items():
            if qualified_table.startswith("_"):
                continue
                
            # Create documents for each query pattern
            query_patterns = table_info.get("query_patterns", {})
            for pattern_name, pattern_info in query_patterns.items():
                # Handle both dict and string pattern info
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
                                "fund_name": fund_name,
                                "qualified_table": qualified_table,
                                "chunk_type": "query_pattern",
                                "pattern_name": pattern_name,
                                "keywords": pattern_info.get('description', ''),
                            },
                        )
                    )
        
        for raw_table_name, df in fund_data.items():
            schema, table, qualified = _infer_qualified_table_name(raw_table_name)
            meta = _get_table_meta(qualified)
            synonyms = ", ".join(meta.get("synonyms", [])[:10])
            date_columns = _get_date_columns_for_table(qualified)

            # Coerce date columns if present
            df = _coerce_datetime_columns(df, date_columns)

            # 1) Table-level summary document
            table_summary_text = _create_table_summary(df, qualified)
            documents.append(
                Document(
                    page_content=table_summary_text,
                    metadata={
                        "fund_name": fund_name,
                        "schema": schema,
                        "table": table,
                        "qualified_table": qualified,
                        "chunk_type": "table_summary",
                        "time_scope": "",
                        "keywords": synonyms,
                    },
                )
            )

            # 2) Monthly summaries if any date column available
            month_col = None
            # First check for nav_date (added by join in embed_from_db.py)
            if "nav_date" in df.columns:
                month_col = "nav_date"
            else:
                # Otherwise use catalog date columns
                for c in date_columns:
                    if c in df.columns:
                        month_col = c
                        break
            
            # Convert to datetime if needed
            if month_col and month_col in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df[month_col]):
                    try:
                        df[month_col] = pd.to_datetime(df[month_col], errors='coerce')
                    except Exception:
                        month_col = None
            
            if month_col and pd.api.types.is_datetime64_any_dtype(df[month_col]):
                for month_label, summary_text in _build_monthly_summary(df, qualified, month_col):
                    documents.append(
                        Document(
                            page_content=summary_text,
                            metadata={
                                "fund_name": fund_name,
                                "schema": schema,
                                "table": table,
                                "qualified_table": qualified,
                                "chunk_type": "monthly_summary",
                                "time_scope": month_label,
                                "keywords": synonyms,
                            },
                        )
                    )

                # 3) Representative example rows per month
                try:
                    df["__month__"] = df[month_col].dt.to_period("M").dt.to_timestamp()
                    for month_value, gdf in df.groupby("__month__", dropna=True):
                        month_label = _format_month(month_value) or ""
                        top_df = _select_representative_rows(gdf, EXAMPLE_ROWS_PER_MONTH).drop(columns=["__month__"], errors="ignore")
                        if top_df.empty:
                            continue
                        preview = top_df.to_string(max_rows=EXAMPLE_ROWS_PER_MONTH)
                        header = f"Representative rows for {qualified} @ {month_label}"
                        documents.append(
                            Document(
                                page_content=f"{header}\n{preview}",
                                metadata={
                                    "fund_name": fund_name,
                                    "schema": schema,
                                    "table": table,
                                    "qualified_table": qualified,
                                    "chunk_type": "examples",
                                    "time_scope": month_label,
                                    "keywords": synonyms,
                                },
                            )
                        )
                except Exception:
                    pass

        # Create/update the collection and insert embeddings in one call
        PGVector.from_documents(
            documents=documents,
            embedding=embeddings,
            connection_string=connection_string,
            collection_name=COLLECTION_NAME,
            pre_delete_collection=bool(force_overwrite),
        )
        return True
    except Exception as e:
        print(f"Error storing fund embeddings: {str(e)}")
        return False


def perform_semantic_search(query: str, top_k: int = 5, tables: Optional[List[str]] = None, month: Optional[str] = None, prefer_chunk_types: Optional[List[str]] = None) -> List[Document]:
    try:
        embeddings = OpenAIEmbeddings(
            model="text-embedding-3-small",
            api_key=OPENAI_API_KEY,
        )
        connection_string = _pg_connection_string()
        vectorstore = PGVector(
            connection_string=connection_string,
            collection_name=COLLECTION_NAME,
            embedding_function=embeddings,
        )

        # Optional metadata filter (works for equality matches)
        filter_meta: Dict[str, str] = {}
        if month:
            filter_meta["time_scope"] = month

        # If only one table provided, we can filter directly; if many, we'll filter client-side
        client_side_tables: Optional[List[str]] = None
        if tables:
            if len(tables) == 1:
                _, _, qualified = _infer_qualified_table_name(tables[0])
                filter_meta["qualified_table"] = qualified
            else:
                client_side_tables = [
                    _infer_qualified_table_name(t)[2] for t in tables
                ]

        # Use MMR for diversity
        try:
            if filter_meta:
                results = vectorstore.max_marginal_relevance_search(query=query, k=max(top_k * 2, 10), lambda_mult=0.4, filter=filter_meta)
            else:
                results = vectorstore.max_marginal_relevance_search(query=query, k=max(top_k * 2, 10), lambda_mult=0.4)
        except Exception:
            if filter_meta:
                results = vectorstore.similarity_search(query=query, k=max(top_k * 2, 10), filter=filter_meta)
            else:
                results = vectorstore.similarity_search(query=query, k=max(top_k * 2, 10))

        # Client-side table filtering when necessary
        if client_side_tables:
            results = [d for d in results if d.metadata.get("qualified_table") in client_side_tables]

        # Reorder by preferred chunk types
        if prefer_chunk_types is None:
            # Prioritize query patterns for better SQL generation
            prefer_chunk_types = ["query_pattern", "monthly_summary", "table_summary", "examples"]
        priority = {t: i for i, t in enumerate(prefer_chunk_types)}
        results.sort(key=lambda d: priority.get(d.metadata.get("chunk_type", "examples"), 99))
        return results[:top_k]
    except Exception as e:
        print(f"Error performing search: {str(e)}")
        return []