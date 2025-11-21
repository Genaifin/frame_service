import os

import json

from openai import AsyncOpenAI

from typing import Dict, List, Optional, Any

import pandas as pd

from .vector_store import perform_semantic_search

from .db_operations import execute_query, get_schema_info, get_enhanced_schema_info, get_latest_dates

from .query_validator import validate_query, format_validation_report

from dotenv import load_dotenv

import re

from datetime import datetime

load_dotenv(override=True)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

OPENAI_MODEL_NAME = os.getenv("OPENAI_MODEL_NAME")

if not OPENAI_API_KEY:

    raise ValueError(
        "OPENAI_API_KEY environment variable is not set. Please check your .env file."
    )

if not os.getenv("OPENAI_MODEL_NAME"):

    print(
        f"⚠️  OPENAI_MODEL_NAME not set in environment. Using default: {OPENAI_MODEL_NAME}"
    )


def get_openai_client():

    return AsyncOpenAI(api_key=OPENAI_API_KEY)


def _round_numbers_in_response(obj: Any, decimal_places: int = 2) -> Any:
    """
    Recursively rounds all numeric values in a response object to specified decimal places.
    
    This function handles:
    - Dictionaries (recursively processes all values)
    - Lists (recursively processes all items)
    - Numbers (rounds float/int to decimal places)
    - Other types (returns as-is)
    
    Args:
        obj: The object to process (dict, list, number, or any other type)
        decimal_places: Number of decimal places to round to (default: 2)
    
    Returns:
        The same object structure with all numbers rounded
    """
    if isinstance(obj, dict):
        # Recursively process all dictionary values
        return {key: _round_numbers_in_response(value, decimal_places) for key, value in obj.items()}
    
    elif isinstance(obj, list):
        # Recursively process all list items
        return [_round_numbers_in_response(item, decimal_places) for item in obj]
    
    elif isinstance(obj, float):
        # Round float values to specified decimal places
        return round(obj, decimal_places)
    
    elif isinstance(obj, int) and not isinstance(obj, bool):
        # Keep integers as-is (no rounding needed, but convert to float if needed)
        return obj
    
    else:
        # Return all other types as-is (strings, booleans, None, etc.)
        return obj


def _extract_key_terms_from_question(question: str) -> Dict[str, Any]:
    """

    Extract key terms and patterns from the question to guide SQL generation

    """

    question_lower = question.lower()

    # Detect time periods

    months = {
        'january': '01',
        'february': '02',
        'march': '03',
        'april': '04',
        'may': '05',
        'june': '06',
        'july': '07',
        'august': '08',
        'september': '09',
        'october': '10',
        'november': '11',
        'december': '12',
        'jan': '01',
        'feb': '02',
        'mar': '03',
        'apr': '04',
        'jun': '06',
        'jul': '07',
        'aug': '08',
        'sep': '09',
        'oct': '10',
        'nov': '11',
        'dec': '12'
    }

    detected_months = []

    for month_name, month_num in months.items():

        if month_name in question_lower:

            detected_months.append((month_name, month_num))

    # Detect percentage patterns

    percent_patterns = re.findall(r'(\d+)\s*%', question)

    # Detect comparison keywords

    comparison_terms = {
        'exceeded': '>',
        'more than': '>',
        'greater than': '>',
        'less than': '<',
        'between': 'BETWEEN',
        'within': 'BETWEEN'
    }

    detected_comparisons = []

    for term, operator in comparison_terms.items():

        if term in question_lower:

            detected_comparisons.append((term, operator))

    # Detect calculation types

    calculations = {
        '% change': 'percentage_change',
        'percentage change': 'percentage_change',
        'percent change': 'percentage_change',
        'difference': 'difference',
        'total': 'sum',
        'largest': 'max',
        'smallest': 'min',
        'contribution': 'sum'
    }

    detected_calculations = []

    for calc_term, calc_type in calculations.items():

        if calc_term in question_lower:

            detected_calculations.append((calc_term, calc_type))

    # Detect entity types

    entities = {
        'law firm': 'Legal',
        'bank': 'Bank',
        'securities': 'portfolio_valuation',
        'asset class': 'inv_type',
        'management fee': 'Management',
        'expense': 'Expense',
        'fees': 'Fee'
    }

    detected_entities = []

    for entity, hint in entities.items():

        if entity in question_lower:

            detected_entities.append((entity, hint))

    return {
        'months': detected_months,
        'percentages': percent_patterns,
        'comparisons': detected_comparisons,
        'calculations': detected_calculations,
        'entities': detected_entities
    }


def _build_info_card_data(title: str,
                          text: str,
                          sub_text: str,
                          use_subtext_parameters: bool = False) -> Dict:

    status_obj = {
        "text": text,
        "subText": sub_text,
        "timeTaken": "",
        "textProperties": {
            "color": "#EA580C",
            "fontSize": "14px"
        }
    }

    if use_subtext_parameters:

        status_obj["subTextParameters"] = {
            "fontSize": "10px",
            "color": "#64748B"
        }

    else:

        status_obj["subTextProperties"] = {
            "fontSize": "10px",
            "color": "#64748B"
        }

    return {
        "icon": "Move3D",
        "title": title,
        "cardBackgroundColor": "#FFF7ED",
        "status": status_obj,
        "actions": []
    }


def _build_clarification_response(clarification_text: str) -> Dict:

    message = clarification_text or "I need a bit more detail to answer this. Please specify date scope (latest, specific month, or all data), the metric you want, and optional grouping."

    response = {"response": {"text": message, "username": "admin"}}
    
    # Round all numbers in the response to 2 decimal places
    return _round_numbers_in_response(response)


def _detect_chart_request(question: str) -> Dict:
    """
    Detect if user is asking for a chart/visualization
    
    Only detects explicit chart keywords: "chart", "graph", "bar chart", "bar graph", "line chart", "plot"
    
    Returns:
        {
            "wants_chart": bool,
            "chart_type": str (bar or line),
            "confidence": int (0-100)
        }
    """
    question_lower = question.lower()
    
    # ONLY these keywords trigger chart detection (as per user requirement)
    # Order matters: check longer phrases FIRST to avoid partial matches
    chart_keywords = [
        ('bar chart', {'type': 'bar', 'weight': 100}),
        ('bar graph', {'type': 'bar', 'weight': 100}),
        ('line chart', {'type': 'line', 'weight': 100}),
        ('plot', {'type': 'line', 'weight': 80}),   # Plot typically means line chart
        ('chart', {'type': 'bar', 'weight': 80}),   # Default to bar for generic "chart"
        ('graph', {'type': 'bar', 'weight': 80}),   # Default to bar for generic "graph"
    ]
    
    detected_chart_type = 'bar'
    max_confidence = 0
    
    # Check for explicit chart keywords ONLY (check longer phrases first)
    for keyword, info in chart_keywords:
        if keyword in question_lower:
            # Use the first match with highest weight
            if info['weight'] > max_confidence:
                detected_chart_type = info['type']
                max_confidence = info['weight']
    
    return {
        "wants_chart": max_confidence > 0,
        "chart_type": detected_chart_type,
        "confidence": max_confidence
    }


def _build_chart_response(answer_text: str, sql_query: str, query_results: List[Dict], question: str, chart_type: str = 'bar') -> Dict:
    
    if not query_results or len(query_results) == 0:
        response = {
            "response": {
                "text": "No data available to create the chart",
                "username": "admin"
            }
        }
        # Round all numbers in the response to 2 decimal places
        return _round_numbers_in_response(response)
    
    # Try to detect date and value columns
    date_column = None
    value_column = None
    
    for col in query_results[0].keys():
        col_lower = col.lower()
        if any(term in col_lower for term in ['date', 'month', 'period', 'time', 'year']):
            date_column = col
        elif any(term in col_lower for term in ['total', 'amount', 'value', 'sum', 'fees', 'expense', 'revenue', 'balance']):
            value_column = col
    
    # If we couldn't auto-detect, use first two columns
    if not date_column:
        date_column = list(query_results[0].keys())[0]
    if not value_column:
        columns = list(query_results[0].keys())
        value_column = columns[1] if len(columns) > 1 else columns[0]
    
    # Build AG Charts data format (array of objects with month/value pairs)
    chart_data_points = []
    data_values = []
    
    for row in query_results:
        date_val = row.get(date_column)
        value_val = row.get(value_column)
        
       # Format date label
        if date_val:
            try:
                # Format as month only (e.g., "Jan")
                from datetime import datetime
                if isinstance(date_val, str):
                    dt = datetime.strptime(str(date_val), '%Y-%m-%d')
                else:
                    dt = date_val
                date_label = dt.strftime('%b')
            except Exception:
                date_label = str(date_val)
        else:
            date_label = "N/A"
        
        # Extract numeric value and round to 2 decimal places
        try:
            numeric_value = round(float(value_val), 2) if value_val is not None else 0
        except Exception:
            numeric_value = 0
        
        # AG Charts format: each data point is an object
        chart_data_points.append({
            "month": date_label,
            "value": numeric_value
        })
        data_values.append(numeric_value)
    
    # Calculate metadata
    total = sum(data_values)
    average = total / len(data_values) if data_values else 0
    max_val = max(data_values) if data_values else 0
    min_val = min(data_values) if data_values else 0
    
    # Determine chart title from question
    chart_title = "Data Trend"
    y_label = "Value"
    if "legal" in question.lower():
        chart_title = "Legal Fees Trend"
        y_label = "Legal Fees"
    elif "management fee" in question.lower():
        chart_title = "Management Fees Trend"
        y_label = "Management Fees"
    elif "expense" in question.lower():
        chart_title = "Expense Trend"
        y_label = "Expenses"
    elif "revenue" in question.lower():
        chart_title = "Revenue Trend"
        y_label = "Revenue"
    
    # Determine date range for display
    first_date = chart_data_points[0]["month"] if chart_data_points else ""
    last_date = chart_data_points[-1]["month"] if chart_data_points else ""
    date_range = f"{first_date} - {last_date}" if first_date and last_date else ""
    
    # Determine chart component name based on type
    chart_component_name = "SimpleBarChart" if chart_type == "bar" else "SimpleLineChart"
    
    # Build AG Charts format chart module (supports bar or line)
    response = {
        "response": {
            "question": question,
            "text": "",
            "username": "admin",
            "modules": [
                {
                    "moduleType": "chart",  #  Use "chart" not "barChart"
                    "data": {
                        "icon": "Move3D",
                        "title": chart_title,
                        "date": date_range,
                        "chartConfig": {  #  AG Charts structure
                            "data": chart_data_points,  #  Array of objects
                            "series": [{  #  AG Charts series config
                                "type": chart_type,  # ✅ Dynamic: "bar" or "line"
                                "xKey": "month",
                                "yKey": "value",
                                "yName": y_label,
                                "label": {
                                    "enabled": False,
                                    "formatter": "({ value }) => value.toFixed(1) + 'k'"
                                }
                            }],
                            "legend": {
                                "enabled": False
                            },
                            "axes": [  #  AG Charts axes config
                                {
                                    "type": "category",
                                    "position": "bottom",
                                    "label": {
                                        "formatter": "",
                                        "rotation": 0
                                    }
                                },
                                {
                                    "type": "number",
                                    "position": "left",
                                    "label": {
                                        "formatter": "k"  # Format as thousands (e.g., "10k")
                                    }
                                }
                            ]
                        },
                        "name": chart_component_name  # ✅ Dynamic component name
                    }
                }
            ],
            "metadata": {  #  Metadata at response level
                "total": total,
                "average": average,
                "max": max_val,
                "min": min_val,
                "period": date_range,
                "data_points": len(data_values)
            }
        }
    }
    
    # Round all numbers in the response to 2 decimal places
    return _round_numbers_in_response(response)


def _build_nested_table_data(rows: List[Dict]) -> Dict:

    if not rows:
        return {
            "colsToShow": [],
            "columnConfig": {},
            "rowData": [],
        }

    cols = list(rows[0].keys())

    column_config = {}
    numeric_columns = []

    for c in cols:

        col_conf = {"name": c.replace('_', ' ').title()}

        # Check if column contains numeric values by checking first row
        is_numeric = False
        try:
            # Check if the value is a number (int, float, or Decimal from database)
            first_value = rows[0].get(c)
            
            # Print debug info for all columns
            print(f"🔍 Column '{c}': value={first_value}, type={type(first_value).__name__}")
            
            # Check for numeric types (including Decimal from database)
            if first_value is not None:
                try:
                    # Try to convert to float - if successful, it's numeric
                    float(first_value)
                    # Exclude boolean values and date strings
                    if not isinstance(first_value, bool) and not isinstance(first_value, str):
                        is_numeric = True
                        numeric_columns.append(c)
                        print(f"✅ Detected numeric column: {c} with value type {type(first_value).__name__}")
                except (ValueError, TypeError):
                    # Not a numeric value
                    pass
        except Exception as e:
            print(f"❌ Error checking numeric type for column {c}: {e}")

        if is_numeric:
            col_conf["type"] = "numericColumn"
            
            # Add valueFormatter for numeric columns to format numbers with 2 decimal places and comma separators
            col_conf["valueFormatter"] = [
                "function",
                "(params) => { if (params.value === null || params.value === undefined) return ''; const num = parseFloat(params.value); if (isNaN(num)) return params.value; return num.toFixed(2).replace(/\\B(?=(\\d{3})+(?!\\d))/g, ','); }"
            ]
            print(f"✅ Added valueFormatter to column: {c}")

        column_config[c] = col_conf

    print(f"📊 Total numeric columns detected: {len(numeric_columns)} - {numeric_columns}")

    # Round all numeric values in rowData to 2 decimal places
    rounded_rows = []
    for row in rows:
        rounded_row = {}
        for key, value in row.items():
            # Try to round any numeric value
            if value is not None:
                try:
                    # Try to convert to float and round
                    numeric_val = float(value)
                    if not isinstance(value, bool) and not isinstance(value, str):
                        rounded_row[key] = round(numeric_val, 2)
                        continue
                except (ValueError, TypeError):
                    pass
            # If not numeric, keep original value
            rounded_row[key] = value
        rounded_rows.append(rounded_row)

    print(f"✅ Rounded {len(rounded_rows)} rows")

    return {
        "colsToShow": cols,
        "columnConfig": column_config,
        "rowData": rounded_rows,
    }


def _analyze_sql_error(error_msg: str, sql_query: str, question: str) -> Dict:
    """Analyze SQL errors and provide specific recovery suggestions"""

    analysis = {"error_type": "unknown", "suggestion": "", "confidence": 0}

    error_lower = error_msg.lower()

    sql_lower = sql_query.lower()

    # Column not found errors

    if "column" in error_lower and "does not exist" in error_lower:

        # Extract column name from error

        col_match = re.search(r'column "([^"]+)"', error_msg)

        if col_match:

            col_name = col_match.group(1)

            if col_name in ["amount", "total", "value", "price", "percent"]:

                analysis["error_type"] = "alias_in_where"

                analysis[
                    "suggestion"] = f"Column '{col_name}' is likely an alias. Use a nested CTE to reference it in WHERE clause"

                analysis["confidence"] = 90

            else:

                analysis["error_type"] = "invalid_column"

                analysis[
                    "suggestion"] = f"Column '{col_name}' doesn't exist. Check the schema for correct column names"

                analysis["confidence"] = 80

    # Date format errors

    elif "date/time field value out of range" in error_lower or "invalid input syntax for type date" in error_lower:

        analysis["error_type"] = "date_format"

        analysis[
            "suggestion"] = "Check date format. Use 'YYYY-MM-DD' format and ensure month-end dates are correct (e.g., '2024-02-29' for February 2024)"

        analysis["confidence"] = 95

    # Missing table join errors

    elif "missing from-clause entry" in error_lower:

        analysis["error_type"] = "missing_join"

        analysis[
            "suggestion"] = "Table alias used without proper JOIN. Ensure all table aliases are defined in FROM/JOIN clauses"

        analysis["confidence"] = 85

    # Division by zero

    elif "division by zero" in error_lower:

        analysis["error_type"] = "division_by_zero"

        analysis[
            "suggestion"] = "Use NULLIF(denominator, 0) to handle division by zero cases"

        analysis["confidence"] = 100

    # Type casting errors

    elif "cannot cast" in error_lower or "invalid input syntax for type" in error_lower:

        analysis["error_type"] = "type_cast"

        analysis[
            "suggestion"] = "Type conversion error. Ensure numeric conversions use ::numeric and handle NULL/empty values"

        analysis["confidence"] = 85

    return analysis


def _analyze_empty_result(sql_query: str, question: str) -> Dict:
    """Analyze why a query returned empty results and suggest fixes"""

    analysis = {"likely_error": False, "reason": "", "suggestion": ""}

    if not sql_query:

        return analysis

    sql_lower = sql_query.lower()

    question_lower = question.lower()

    # Check 1: Using plural forms for type column

    if re.search(r"type\s*=\s*['\"]expenses['\"]", sql_lower):

        analysis["likely_error"] = True

        analysis[
            "reason"] = "Using plural 'Expenses' instead of singular 'Expense' in type column"

        analysis[
            "suggestion"] = "Change type = 'Expenses' to type = 'Expense' (singular)"

        return analysis

    if re.search(r"type\s*=\s*['\"]revenues['\"]", sql_lower):

        analysis["likely_error"] = True

        analysis[
            "reason"] = "Using plural 'Revenues' instead of singular 'Revenue' in type column"

        analysis[
            "suggestion"] = "Change type = 'Revenues' to type = 'Revenue' (singular)"

        return analysis

    # Check 2: Searching for expense names in wrong column (accounting_head instead of financial_account)

    expense_keywords = [
        "legal", "audit", "bank", "admin", "custodian", "management",
        "performance", "fee"
    ]

    for keyword in expense_keywords:

        if keyword in question_lower and "accounting_head" in sql_lower and f"like '%{keyword}%'" in sql_lower:

            analysis["likely_error"] = True

            analysis[
                "reason"] = f"Searching for '{keyword}' in accounting_head column (contains technical codes, not account names)"

            analysis[
                "suggestion"] = f"Use financial_account column instead: WHERE financial_account LIKE '%{keyword.title()}%'"

            return analysis

    # Check 3: Missing proper joins for date filtering

    if ("trial_balance" in sql_lower or "portfolio_valuation" in sql_lower
            or "dividend" in sql_lower):

        if "file_date" in sql_lower:

            if "navpack_version" not in sql_lower or "nav_pack" not in sql_lower:

                analysis["likely_error"] = True

                analysis[
                    "reason"] = "Date filtering attempted without proper joins through navpack_version and nav_pack"

                analysis[
                    "suggestion"] = "Add joins: JOIN navpack_version nv ON table.navpack_version_id = nv.navpack_version_id JOIN nav_pack np ON nv.navpack_id = np.navpack_id WHERE np.file_date = ..."

                return analysis

    # Check 4: Question asks for expense data but query might be misconfigured

    if any(word in question_lower
           for word in ["expense", "fee", "cost", "paid"]):

        if "type" in sql_lower and "expense" not in sql_lower:

            analysis["likely_error"] = True

            analysis[
                "reason"] = "Question asks about expenses but query doesn't filter for type = 'Expense'"

            analysis[
                "suggestion"] = "Add WHERE type = 'Expense' to filter for expense records"

            return analysis

    return analysis


def _build_multiple_module_response(answer_text: str, sql_query: str,
                                    query_results: List[Dict], question: str = "") -> Dict:

    # Detect if user wants a chart visualization
    chart_detection = _detect_chart_request(question)
    
    # Check if this is time series data (has date column and numeric column)
    has_date_column = False
    has_numeric_column = False
    
    if query_results and len(query_results) > 0:
        for col in query_results[0].keys():
            col_lower = col.lower()
            if any(term in col_lower for term in ['date', 'month', 'period', 'time', 'year']):
                has_date_column = True
            if any(term in col_lower for term in ['total', 'amount', 'value', 'sum', 'fees', 'expense', 'revenue', 'balance']):
                has_numeric_column = True
    
    is_time_series_data = has_date_column and has_numeric_column and len(query_results) > 1
    
    # If user explicitly asked for a chart and data is suitable for visualization
    if chart_detection["wants_chart"] and is_time_series_data:
        return _build_chart_response(answer_text, sql_query, query_results, question, chart_detection["chart_type"])
    
    # Check if this is a simple one-line answer (single value result)

    is_simple_answer = query_results and len(query_results) == 1 and len(
        query_results[0].keys()) == 1

    if is_simple_answer:

        # Return textCard format for simple answers

        key = list(query_results[0].keys())[0]

        value_num = query_results[0][key]

        try:

            value_fmt = f"{float(value_num):,.2f}"

        except Exception:

            value_fmt = str(value_num)

        # Create a short, conversational answer based on the metric name

        metric_name = key.replace('_', ' ').lower()

        answer_text_final = f"The {metric_name} is {value_fmt}."

        response = {"response": {"text": answer_text_final, "username": "admin"}}
        
        # Round all numbers in the response to 2 decimal places
        return _round_numbers_in_response(response)

    else:

        # Return nestedTable format for complex/tabular answers

        summary_text = "Here are the results of your query"

        if not query_results:

            summary_text = "No Data found by the SQL query"

        response = {
            "response": {
                "question":
                answer_text,
                "text":
                summary_text,
                "modules": [{
                    "moduleType":
                    "nestedTable",
                    "data":
                    _build_nested_table_data(query_results or [])
                }]
            }
        }
        
        # Round all numbers in the response to 2 decimal places
        return _round_numbers_in_response(response)


def _format_schema_for_prompt(schema_info: Dict) -> str:
    """Format schema information in a clear, structured way for the LLM"""

    formatted = "DATABASE SCHEMA:\n\n"

    # Add live distinct values first for critical columns

    formatted += "=== ACTUAL DATABASE VALUES (from live data) ===\n"

    formatted += "CRITICAL: You MUST use one of these EXACT values. Do not guess or pluralize!\n\n"

    if "nexbridge.trial_balance" in schema_info:

        live_vals = schema_info["nexbridge.trial_balance"].get(
            "live_distinct_values", {})

        if "type" in live_vals:

            formatted += f"trial_balance.type EXACT values: {', '.join(live_vals['type'])}\n"

        if "category" in live_vals:

            formatted += f"trial_balance.category distinct values (can be NULL): {', '.join(live_vals['category'][:15])}\n"

    if "nexbridge.portfolio_valuation" in schema_info:

        live_vals = schema_info["nexbridge.portfolio_valuation"].get(
            "live_distinct_values", {})

        if "inv_type" in live_vals:

            formatted += f"portfolio_valuation.inv_type distinct values: {', '.join(live_vals['inv_type'])}\n"

    formatted += "\n"

    # Add global patterns

    global_patterns = schema_info.get("_global_patterns", {})

    if global_patterns:

        formatted += "=== CRITICAL: DATE FILTERING PATTERNS ===\n"

        date_filtering = global_patterns.get("date_filtering", {})

        if date_filtering:

            formatted += f"{date_filtering.get('description', '')}\n\n"

            formatted += f"Standard Join Pattern:\n{date_filtering.get('standard_join_pattern', '')}\n\n"

            formatted += f"Example:\n{date_filtering.get('example', '')}\n\n"

        formatted += "=== HANDLING NULL VALUES ===\n"

        null_handling = global_patterns.get("handling_nulls", {})

        if null_handling:

            formatted += f"{null_handling.get('description', '')}\n"

            formatted += f"Example: {null_handling.get('example', '')}\n\n"

    formatted += "=== TABLES ===\n\n"

    for table_name, table_info in schema_info.items():

        if table_name.startswith("_"):

            continue

        formatted += f"Table: {table_name}\n"

        formatted += f"Description: {table_info.get('description', 'N/A')}\n"

        formatted += f"Synonyms: {', '.join(table_info.get('synonyms', []))}\n"

        # Date relationship info

        date_rel = table_info.get('date_relationship_info', '')

        if date_rel:

            formatted += f"DATE FILTERING: {date_rel}\n"

        formatted += "\nColumns:\n"

        for col in table_info.get('columns', []):

            col_desc = f"  - {col['name']} ({col['type']}): {col.get('description', '')}\n"

            formatted += col_desc

            # Add JSON structure if available

            if 'json_structure' in col:

                formatted += f"    JSON Structure: {json.dumps(col['json_structure'], indent=6)}\n"

            # Add usage notes if available

            if 'usage_notes' in col:

                formatted += f"    Usage: {'; '.join(col['usage_notes'])}\n"

            # Add extraction example if available

            if 'extraction_example' in col:

                formatted += f"    Example: {col['extraction_example']}\n"

        relationships = table_info.get('relationships', [])

        if relationships:

            formatted += "\nRelationships:\n"

            for rel in relationships:

                formatted += f"  - {rel.get('from', '')} → {rel.get('to', '')} ({rel.get('type', '')})\n"

        common_queries = table_info.get('common_queries', [])

        if common_queries:

            formatted += f"\nCommon Queries: {', '.join(common_queries)}\n"

        formatted += "\n" + "=" * 80 + "\n\n"

    return formatted


async def prompt_openai_with_rag(schema_info: Dict, question: str,
                                 previous_data: List[Dict],
                                 semantic_context: str,
                                 fund_id: Optional[int] = None) -> Dict:
    """Generate SQL query using OpenAI with enhanced prompting and schema information"""

    try:

        client = get_openai_client()

        # Get the latest date information

        latest_dates = get_latest_dates()

        # Format previous iterations' data if available

        additional_context = ""

        if previous_data and len(previous_data) > 0:

            additional_context = "Previous query attempts and their results:\n\n"

            for iteration_data in previous_data:

                iteration_num = iteration_data["iteration"]

                query = iteration_data["query"]

                results = iteration_data["results"]

                # Check if there was an error

                if isinstance(results, dict) and "error" in results:

                    additional_context += f"Iteration {iteration_num}:\n"

                    additional_context += f"Query: {query}\n"

                    additional_context += f"ERROR: {results['error']}\n"

                    additional_context += "Fix this error in the next attempt!\n\n"

                else:

                    # Only include a sample of the results if there are many

                    if len(results) > 5:

                        results_sample = results[:5]

                        additional_context += f"Iteration {iteration_num}:\n"

                        additional_context += f"Query: {query}\n"

                        additional_context += f"Results (showing 5/{len(results)} rows): {json.dumps(results_sample, indent=2)}\n\n"

                    else:

                        additional_context += f"Iteration {iteration_num}:\n"

                        additional_context += f"Query: {query}\n"

                        additional_context += f"Results: {json.dumps(results, indent=2)}\n\n"

        # Format schema information

        formatted_schema = _format_schema_for_prompt(schema_info)

        # Print all inputs going into the prompt

        print("\n" + "=" * 80)

        print("PROMPT INPUTS TRACKING")

        print("=" * 80)

        print(f"\n1. USER QUESTION:\n{question}\n")

        print(f"\n2. LATEST DATES:\n{latest_dates}\n")

        print(f"\n3. SEMANTIC SEARCH CONTEXT:\n{semantic_context}\n")

        print(
            f"\n4. ADDITIONAL CONTEXT (Previous Iterations):\n{additional_context if additional_context else 'None'}\n"
        )

        print(
            f"\n5. FORMATTED SCHEMA (first 1000 chars):\n{formatted_schema[:1000]}...\n"
        )

        print(f"\n6. SCHEMA INFO KEYS: {list(schema_info.keys())}\n")

        print("=" * 80 + "\n")

        # Build fund filtering section if fund_id is provided
        fund_filter_section = ""
        if fund_id:
            fund_filter_section = f"""
=== FUND FILTERING (CRITICAL) ===
This query MUST be filtered for a specific fund:
- Fund ID: {fund_id}
- ALWAYS add this filter: WHERE np.source_id = {fund_id}
- Include this filter in ALL joins to nav_pack table
- This filter is MANDATORY - do not omit it

Example join pattern with fund filter:
WITH latest_versions AS (
  SELECT navpack_id, MAX(version) as latest_version
  FROM nexbridge.navpack_version
  GROUP BY navpack_id
)
SELECT ...
FROM nexbridge.trial_balance tb
JOIN nexbridge.navpack_version nv ON tb.navpack_version_id = nv.navpack_version_id
JOIN latest_versions lv ON nv.navpack_id = lv.navpack_id AND nv.version = lv.latest_version
JOIN nexbridge.nav_pack np ON nv.navpack_id = np.navpack_id
WHERE np.source_id = {fund_id}  -- REQUIRED FUND FILTER
  AND np.file_date >= (SELECT MAX(file_date) - INTERVAL '5 months' FROM nexbridge.nav_pack WHERE source_id = {fund_id})

"""
        
        # Create the enhanced prompt

        prompt = f"""You are an expert PostgreSQL query generator for financial fund data.

{formatted_schema}

LATEST DATA AVAILABILITY:

{latest_dates}

{fund_filter_section}
SEMANTIC SEARCH CONTEXT (relevant data patterns):

{semantic_context}

ADDITIONAL CONTEXT: {additional_context}

USER QUESTION: {question}

CRITICAL RULES - READ CAREFULLY BEFORE GENERATING ANY QUERY

0. CHART/VISUALIZATION REQUESTS (CRITICAL - CHECK FIRST):

   If the user asks for a "chart", "graph", "bar chart", "plot", etc.:
   
   REQUIRED QUERY FORMAT for charts:
   - Query MUST return time series data with a date column and a numeric value column
   - Date column should be named with "date", "month", "period", or "time" in the name
   - Value column should be named with "total", "amount", "value", "fees", or metric name
   - Order results by date chronologically (ORDER BY date_column ASC)
   - Return multiple rows (at least 2-3 data points for meaningful visualization)
   - MUST include latest_versions CTE and fund_id filter
   
   CORRECT COMPLETE EXAMPLE for chart queries:
   WITH latest_versions AS (
     SELECT navpack_id, MAX(version) as latest_version
     FROM nexbridge.navpack_version
     GROUP BY navpack_id
   )
   SELECT 
     np.file_date,
     SUM(tb.ending_balance) AS total_legal_fees
   FROM nexbridge.trial_balance tb
   JOIN nexbridge.navpack_version nv ON tb.navpack_version_id = nv.navpack_version_id
   JOIN latest_versions lv ON nv.navpack_id = lv.navpack_id AND nv.version = lv.latest_version
   JOIN nexbridge.nav_pack np ON nv.navpack_id = np.navpack_id
   WHERE np.source_id = {fund_id}  -- CRITICAL: Use actual fund_id parameter
     AND tb.type = 'Expense'
     AND tb.financial_account = 'Legal Expense'
     AND np.file_date >= (SELECT MAX(file_date) - INTERVAL '5 months' FROM nexbridge.nav_pack WHERE fund_id = {fund_id})
   GROUP BY np.file_date
   ORDER BY np.file_date
   
   CRITICAL NOTES for chart queries:
   - ALWAYS include latest_versions CTE to get current data
   - ALWAYS filter by fund_id if provided
   - Use dynamic date calculation (MAX date - interval) instead of hardcoded dates
   - Adapt financial_account, type, and metric names based on user question
   - GROUP BY date column to get one row per time period
   
   WRONG for charts:
   - Single value queries (only 1 row, 1 column) - these are not chartable
   - Queries without date/time dimension
   - Queries without clear numeric metrics
   - Missing latest_versions CTE (will return old data!)
   - Missing fund_id filter (will mix multiple funds!)
   
   Chart keywords (ONLY these trigger chart visualization):
   - "bar chart" or "bar graph" → generates bar chart
   - "line chart" → generates line chart
   - "chart" or "graph" → generates bar chart (default)
   - "plot" → generates line chart
   
   NOTE: Chart visualization is ONLY triggered by these explicit keywords.
         Time-based questions alone (e.g., "last 6 months") will NOT automatically create charts.

1. EXACT ENUM VALUES (trial_balance.type column):

   The type column has ONLY these exact values (ALL SINGULAR):

   CORRECT: 'Assets', 'Liabilities', 'Revenue', 'Expense', 'Capital'

   WRONG: 'Expenses', 'Revenues' (plurals are NEVER valid)

   Examples:

   WHERE type = 'Expense'     -- CORRECT (singular)

   WHERE type = 'Revenue'     -- CORRECT (singular)

   WHERE type = 'Expenses'    -- WRONG (will return 0 rows)

   WHERE type = 'Revenues'    -- WRONG (will return 0 rows)

2. COLUMN SELECTION FOR ACCOUNT NAME SEARCHES (trial_balance):

   When searching for expenses/accounts by NAME (Legal, Audit, Bank Fees, etc.):

   USE: financial_account column

      - Contains human-readable names: "Legal Expense", "Audit Expense", "Bank Fees"

      - Example: WHERE financial_account LIKE '%Legal%'

   DO NOT USE: accounting_head column

      - Contains technical codes: "PSEUDO SECURITIES::NONTRADEEXP"

      - This will NOT match expense names like "Legal" or "Audit"

   CORRECT expense search pattern:

   WHERE type = 'Expense' AND financial_account LIKE '%Legal%'

   WRONG expense search pattern:

   WHERE type = 'Expenses' AND accounting_head LIKE '%legal%'  -- BOTH WRONG!

3. NULL HANDLING (trial_balance):

   - accounting_head: ~20% NULL - use COALESCE or IS NULL checks when filtering

   - category: ~30% NULL - use COALESCE or IS NULL checks when filtering

   - financial_account: 0% NULL - always populated, safe to filter directly

4. DATE FILTERING (trial_balance, portfolio_valuation, dividend):

   These tables don't have date columns. To filter by date:

   MUST join: table → navpack_version → nav_pack

   Filter on: nav_pack.file_date

   Example:

   FROM nexbridge.trial_balance tb

   JOIN nexbridge.navpack_version nv ON tb.navpack_version_id = nv.navpack_version_id

   JOIN nexbridge.nav_pack np ON nv.navpack_id = np.navpack_id

   WHERE np.file_date = '2024-04-30'

5. MONTH-OVER-MONTH COMPARISONS (CRITICAL):

   When user asks about changes "month-over-month" or "for all data":

   CORRECT approach - compare ALL consecutive months:

   - Use window functions (LAG/LEAD) to compare each month to its previous month

   - OR use self-join to compare all month pairs

   - Return ALL months where condition is met, not just latest

   WRONG approach:

   - Filtering for only latest month (WHERE file_date = MAX(file_date))

   - This will miss historical months that also meet the criteria

   Example for "all data":

   WITH monthly_data AS (

     SELECT financial_account, SUM(ending_balance) as total, np.file_date,

            LAG(SUM(ending_balance)) OVER (PARTITION BY financial_account ORDER BY np.file_date) as prev_total

     FROM trial_balance tb

     JOIN navpack_version nv ON tb.navpack_version_id = nv.navpack_version_id

     JOIN nav_pack np ON nv.navpack_id = np.navpack_id

     GROUP BY financial_account, np.file_date

   )

   SELECT * FROM monthly_data WHERE (total - prev_total) / prev_total > 0.5

6. TRANSACTION-LEVEL DETAIL (CRITICAL - APPLIES TO ANY ENTITY NAME):

   The extra_data column in trial_balance contains JSON with transaction-level details:

   Structure: {{"general_ledger": [{{"tran_description": "Entity Name", "local_amount": "123.45"}}]}}

   WHEN TO USE - If question asks "which [ENTITY]" or "for which [ENTITIES]":

   - "which law firms", "which banks", "which brokers", "which vendors", "which counterparties"

   - ANY question asking about SPECIFIC entity names (not just account totals)

   - ANY question comparing INDIVIDUAL entities month-over-month

   - Pattern: If question contains "which [plural noun]", extract individual entities from JSON

   CRITICAL for entity questions:

   - ALWAYS return DISTINCT entity names to avoid duplicates

   - For "which banks" - extract from Bank Fees transactions

   - Bank names in data: 'GS', 'JPM', 'UBS' (not full names)

   HOW TO EXTRACT (basic extraction):

   WITH transactions AS (

     SELECT np.file_date,

            jsonb_array_elements(tb.extra_data::jsonb->'general_ledger') AS ledger_entry

     FROM nexbridge.trial_balance tb

     JOIN nexbridge.navpack_version nv ON tb.navpack_version_id = nv.navpack_version_id

     JOIN nexbridge.nav_pack np ON nv.navpack_id = np.navpack_id

     WHERE tb.type = 'Expense' AND tb.financial_account LIKE '%Bank%'

   ),

   monthly_by_entity AS (

     SELECT file_date,

            ledger_entry->>'tran_description' AS entity_name,

            (ledger_entry->>'local_amount')::numeric AS amount

     FROM transactions

   )

   SELECT * FROM monthly_by_entity

   For month-over-month comparisons, add another CTE with LAG function (see query_patterns)

   CRITICAL: If question asks "which [entities]" or "for which [entities]" (law firms, banks,

   brokers, vendors, etc.) - you MUST extract individual entity names from extra_data JSON,

   NOT just group by financial_account! The word "which" indicates the user wants individual

   entities, not aggregated account totals.

7. ASSET CLASS TERMINOLOGY (CRITICAL):

   "Asset classes" in finance = investment types (equities, bonds, cash, etc.)

   CORRECT TABLE FOR ASSET CLASS QUESTIONS:

   - Use: portfolio_valuation.inv_type

   - Examples: COMMNNLT (equities), CASH, CASHF, PSEUDOBND (bonds), CFD (derivatives)

   - For allocation: SUM(end_book_mv) GROUP BY inv_type

   WRONG TABLE (common mistake):

   - Do NOT use: trial_balance.category where type='Assets'

   - Those are accounting categories, NOT investment asset classes

   Questions that need portfolio_valuation.inv_type:

   - "asset class allocation", "asset classes", "investment types"

   - "portfolio composition", "allocation percentage"

   - ANY question about % of portfolio by investment type

8. SQL ALIAS RULES (CRITICAL):

   You CANNOT use column aliases in WHERE clause of the same SELECT statement!

   WRONG (causes "column does not exist" error):

   SELECT (ledger_entry->>'local_amount')::numeric AS amount

   FROM transactions

   WHERE amount > 1000  -- ERROR: can't use alias in WHERE

   CORRECT - Use nested CTEs:

   WITH monthly_by_entity AS (

     SELECT file_date,

            ledger_entry->>'tran_description' AS entity_name,

            (ledger_entry->>'local_amount')::numeric AS amount

     FROM transactions

   ),

   with_previous AS (

     SELECT *, LAG(amount) OVER (PARTITION BY entity_name ORDER BY file_date) AS prev_amount

     FROM monthly_by_entity

   )

   SELECT * FROM with_previous WHERE amount > prev_amount  -- ✅ CORRECT

9. PERCENTAGE CALCULATION RULES:

   For any percentage change calculations:

   FORMULA: ((new_value - old_value) / ABS(old_value)) * 100

   CRITICAL NOTES:

   - Always use ABS() on denominator to handle negative values correctly

   - Use NULLIF(ABS(old_value), 0) to prevent division by zero

   - Round to 2 decimal places for readability: ROUND(..., 2)

   - For "exceeded X%" questions: percent_change > X

   - For "between X% and Y%" questions: percent_change BETWEEN X AND Y

   EXAMPLE:

   ROUND(((feb_amount - jan_amount) / NULLIF(ABS(jan_amount), 0) * 100), 2) as percent_change

10. DATE HANDLING FOR COMPARISONS:

   When comparing specific months (e.g., Jan vs Feb):

   APPROACH 1 - Using CTEs with specific dates:

   WITH jan_data AS (

     SELECT ... WHERE np.file_date = '2024-01-31'

   ),

   feb_data AS (

     SELECT ... WHERE np.file_date = '2024-02-29'

   )

   SELECT ... FROM jan_data, feb_data

   APPROACH 2 - Using CASE WHEN pivot:

   SELECT

     MAX(CASE WHEN np.file_date = '2024-01-31' THEN value END) as jan_value,

     MAX(CASE WHEN np.file_date = '2024-02-29' THEN value END) as feb_value

   FROM ...

   WHERE np.file_date IN ('2024-01-31', '2024-02-29')

   CRITICAL: Always check actual month-end dates (Jan=31, Feb=28/29, etc.)

11. SECURITIES AND PORTFOLIO QUERIES:

   For portfolio_valuation queries:

   - "Common stock securities" = WHERE inv_type IN ('COMMNNLT', 'COMMNLST') -- includes BOTH listed and unlisted

   - "COMMNNLT" = "Equity UnListed" (common stock non-listed)

   - "COMMNLST" = "Equity Listed" (common stock listed)

   - "Price difference" = Use end_local_market_price for unit price comparisons

   - "Book value difference" = Use end_book_mv for total value comparisons

   - "Largest contributor" = ORDER BY end_book_mv DESC LIMIT 1

   - "NAV contribution" = SUM(end_book_mv)

   INVESTMENT TYPE TERMINOLOGY MAPPING (CRITICAL):

   When user mentions SPECIFIC equity types:

   - "COMMNNLT" → use inv_type = 'COMMNNLT' (Equity UnListed only)

   - "Equity UnListed" → use inv_type = 'COMMNNLT' (unlisted only)

   - "equity unlisted" → use inv_type = 'COMMNNLT' (unlisted only)

   - "COMMNLST" → use inv_type = 'COMMNLST' (Equity Listed only)

   - "Equity Listed" → use inv_type = 'COMMNLST' (listed only)

   - "equity listed" → use inv_type = 'COMMNLST' (listed only)

   When user mentions GENERAL common stock (WITHOUT specifying listed/unlisted):

   - "common stock" → use inv_type IN ('COMMNNLT', 'COMMNLST') -- BOTH types

   - "common stock securities" → use inv_type IN ('COMMNNLT', 'COMMNLST') -- BOTH types

   - "common stocks" → use inv_type IN ('COMMNNLT', 'COMMNLST') -- BOTH types

   RULE: If user says "common stock" or "common stock securities" WITHOUT specifying

   "listed" or "unlisted", you MUST include BOTH types using IN clause.

   ALWAYS use the database codes in SQL queries, NOT the user-friendly names.

   CRITICAL: Distinguish between:

   - end_local_market_price: Price per unit/share

   - end_book_mv: Total market value (price × quantity)

   - For "ending book market price difference" use end_book_mv

   CRITICAL for "largest contributor" questions:

   - Return the INDIVIDUAL security (inv_id), not the sum

   - Use: SELECT inv_id, end_book_mv ... ORDER BY end_book_mv DESC LIMIT 1

   - WRONG: SELECT inv_type, SUM(end_book_mv) ... GROUP BY inv_type

   - CORRECT for specific type: SELECT inv_id, end_book_mv ... WHERE inv_type = 'COMMNNLT' ORDER BY end_book_mv DESC LIMIT 1

   - CORRECT for common stock: SELECT inv_id, end_book_mv ... WHERE inv_type IN ('COMMNNLT', 'COMMNLST') ORDER BY end_book_mv DESC LIMIT 1

   - The question asks for WHICH security, not the total

12. EXPENSE ACCOUNT NAME MAPPING (CRITICAL):

   When searching for expenses, use EXACT account names from the database:

   MAPPINGS:

   - "management fee/fees" → financial_account = 'MgmtFeeExpense'

   - "performance fee/fees" → financial_account = 'PerfFeeExpense'

   - "legal fee/fees" → financial_account = 'Legal Expense'

   - "audit fee/fees" → financial_account = 'Audit Expense'

   - "bank fee/fees" → financial_account = 'Bank Fees'

   - "custodian fee/fees" → financial_account = 'Custodian Fees'

   - "admin/administration fee/fees" → financial_account = 'Fund Administration Fees'

   WRONG: WHERE financial_account LIKE '%Management%'

   CORRECT: WHERE financial_account = 'MgmtFeeExpense'

   CRITICAL: Use exact matches (=) not LIKE when you know the exact account name!

13. LATEST VERSION REQUIREMENT (CRITICAL - MUST ALWAYS USE):
   Each NAV pack can have multiple versions (original upload, corrections, overrides).
   You MUST ALWAYS use the LATEST version unless user explicitly asks for version history.
   STANDARD PATTERN (REQUIRED FOR ALL QUERIES):
   WITH latest_versions AS (
     SELECT navpack_id, MAX(version) as latest_version
     FROM nexbridge.navpack_version
     GROUP BY navpack_id
   )
   SELECT ...
   FROM <data_table> dt
   JOIN nexbridge.navpack_version nv ON dt.navpack_version_id = nv.navpack_version_id
   JOIN latest_versions lv ON nv.navpack_id = lv.navpack_id 
     AND nv.version = lv.latest_version
   JOIN nexbridge.nav_pack np ON nv.navpack_id = np.navpack_id
   CRITICAL NOTES:
   - ALWAYS include latest_versions CTE at the start of EVERY query
   - ALWAYS join with: JOIN latest_versions lv ON nv.navpack_id = lv.navpack_id AND nv.version = lv.latest_version
   - This ensures you get current, accurate data (not outdated versions)
   - Only skip this if user explicitly asks for "all versions" or "version X"
14. DATA SOURCE FILTERING (for multi-source funds):
   The system supports multiple sources (administrators) for the same fund.
   Available sources in database:
   - 'Bluefield' (NexBridge fund administrator)
   - 'Harborview' (ASOF fund administrator)
   - 'ClearLedger' (Alternative ASOF administrator)
   - 'StratusGA' (Stonewell fund administrator)
   - 'VeridexAS' (Alternative Stonewell administrator)
   WHEN TO FILTER BY SOURCE:
   - User explicitly mentions source: "from Harborview", "according to Bluefield"
   - Comparing sources: "compare Harborview vs ClearLedger"
   - Otherwise: DO NOT filter by source (return all sources)
   HOW TO FILTER BY SOURCE:
   JOIN nexbridge.source s ON np.source_id = s.id
   WHERE s.name = 'Bluefield'
   CRITICAL: Source names are case-sensitive. Use exact names from list above.
15. CRITICAL SQL SYNTAX RULES:

   COMMENTS in SQL:

   - NEVER put comments at the end of lines in multiline queries

   - SQL comments can break the query if not properly terminated

   - WRONG: WHERE price > 0  -- Valid prices only)

   - CORRECT: WHERE price > 0

   DISTINCT for entity questions:

   - When asking "for which [entities]" - use SELECT DISTINCT to avoid duplicates

   - Return unique entity names, not all occurrences

   Latest period queries:

   - For "in the total portfolio" without specific date: use latest date

   - Add: WHERE np.file_date = (SELECT MAX(file_date) FROM nexbridge.nav_pack)

   Price comparison periods:

   - "compared to prior period" = ALL consecutive periods, not just last 2

   - Use LAG() window function over ALL periods

   - Don't restrict to specific months unless asked

   Result formatting:

   - For "which [entities]" questions: ALWAYS return inv_id/entity_name with additional columns

   - Even for single result, include descriptive columns (e.g., percent_change, dates)

   - This ensures results are shown in table format, not as single values

16. QUERY VALIDATION CHECKLIST:

   Before finalizing SQL, verify:

   □ ✅ CRITICAL: Included latest_versions CTE to filter to latest version only

   □ ✅ CRITICAL: Joined with latest_versions: JOIN latest_versions lv ON nv.navpack_id = lv.navpack_id AND nv.version = lv.latest_version


   □ Used exact enum values from schema (check possible_values arrays)

   □ Used financial_account (NOT accounting_head) for account name searches

   □ Used singular forms: 'Expense', 'Revenue', 'Assets' (NOT plural)

   □ Included proper NULL handling for nullable columns

   □ Joined through navpack_version → nav_pack for date filtering

   □ Used schema prefix (nexbridge.) for all table names

   □ For "all data" queries: checking ALL time periods, not just latest

   □ If question asks "which [entities]" or "for which [entities]": Extract from extra_data JSON

   □ NOT using column aliases in WHERE clause - use nested CTE instead

   □ For "asset class" questions: Use portfolio_valuation.inv_type, NOT trial_balance.category

   □ For inv_type terminology: Use 'COMMNNLT' for "Equity UnListed", 'COMMNLST' for "Equity Listed"

   □ For "common stock" WITHOUT listed/unlisted specified: Use IN ('COMMNNLT', 'COMMNLST')

   □ NO comments in SQL that could break syntax

   □ Use DISTINCT for entity lists to avoid duplicates

   □ For "largest/smallest" return individual records, not sums
   □ If user mentions source (Bluefield, Harborview, etc.): JOIN source table and filter by s.name

═══════════════════════════════════════════════════════════════════════════════

CRITICAL INSTRUCTIONS:

1. **Column Names**: Use EXACT column names from the schema above. DO NOT use columns that don't exist.

2. **Date Filtering**: For time-based queries on trial_balance, portfolio_valuation, or dividend tables:

   - You MUST join through navpack_version and nav_pack tables

   - Filter using nav_pack.file_date column

   - Example: JOIN nexbridge.navpack_version nv ON tb.navpack_version_id = nv.navpack_version_id JOIN nexbridge.nav_pack np ON nv.navpack_id = np.navpack_id WHERE np.file_date = '2024-04-30'

3. **Latest Month**: When user asks for "latest month", use the MAX file_date from the latest dates above

4. **NULL Handling**: category and accounting_head columns in trial_balance can be NULL - handle appropriately

5. **Schema Prefix**: Always use schema prefix (nexbridge.) for table names

6. **Validation**: Ensure the query will execute without errors

RESPONSE FORMAT (JSON):

{{

    "has_answer": true|false,

    "sql_query": "YOUR COMPLETE POSTGRESQL QUERY HERE",

    "explanation": "Brief explanation of what the query does and how it answers the question",

    "confidence": 0-100

}}

If you cannot answer with confidence, set has_answer=false and explain why in the explanation field.

If previous attempts had errors, analyze the error and generate a corrected query.

"""

        # Print the full prompt being sent

        print("\n" + "=" * 80)

        print("FULL PROMPT BEING SENT TO OPENAI")

        print("=" * 80)

        print(f"\nPrompt Length: {len(prompt)} characters")

        print(f"\nFirst 2000 characters of prompt:\n{prompt[:2000]}")

        print(f"\n...\n\nLast 1000 characters of prompt:\n{prompt[-1000:]}")

        print("=" * 80 + "\n")

        # Get response from OpenAI

        completion = await client.chat.completions.create(
            model=OPENAI_MODEL_NAME,
            seed=42,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[{
                "role":
                "system",
                "content":
                "You are an expert PostgreSQL database specialist. You generate accurate, executable SQL queries for financial fund data. You always use correct column names, proper table joins, and handle edge cases like NULL values. When you see an error from a previous attempt, you analyze it carefully and generate a corrected query."
            }, {
                "role": "user",
                "content": prompt
            }])

        # Get token usage information

        input_tokens = completion.usage.prompt_tokens

        output_tokens = completion.usage.completion_tokens

        # Print OpenAI response

        print("\n" + "=" * 80)

        print("OPENAI RESPONSE RECEIVED")

        print("=" * 80)

        print(f"\nModel: {OPENAI_MODEL_NAME}")

        print(f"Input Tokens: {input_tokens}")

        print(f"Output Tokens: {output_tokens}")

        print(f"Total Tokens: {input_tokens + output_tokens}")

        print(f"\nRaw Response:\n{completion.choices[0].message.content}")

        print("=" * 80 + "\n")

        try:

            # Extract JSON from the response

            response_text = completion.choices[0].message.content

            # Find JSON object in the response

            start_idx = response_text.find('{')

            end_idx = response_text.rfind('}') + 1

            if start_idx >= 0 and end_idx > start_idx:

                json_str = response_text[start_idx:end_idx]

                result = json.loads(json_str)

                # Add token usage information

                result["token_usage"] = {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens
                }

                return result

            else:

                return {
                    "error": "Could not parse JSON from OpenAI response",
                    "raw_response": response_text
                }

        except Exception as e:

            return {
                "error": f"Error processing OpenAI response: {str(e)}",
                "raw_response": completion.choices[0].message.content
            }

    except Exception as e:

        print(f"Error in prompt_openai_with_rag: {str(e)}")

        return {"error": str(e)}


async def answer_question(question: str, fund_id: Optional[int] = None, fund_name: Optional[str] = None) -> Dict:
    """Main function to answer user questions using RAG + SQL generation"""

    try:
        
        # Log fund filtering if provided
        if fund_id and fund_name:
            print(f"🔍 Filtering results for fund '{fund_name}' (ID: {fund_id})")
        elif fund_id:
            print(f"🔍 Filtering results for fund ID: {fund_id}")

        # Extract key terms from the question

        key_terms = _extract_key_terms_from_question(question)

        print(f"Detected key terms: {key_terms}")

        # Create enhanced search query with key terms

        search_query = question

        if key_terms['entities']:

            # Add entity hints to search

            entity_hints = ' '.join(
                [hint for _, hint in key_terms['entities']])

            search_query = f"{question} {entity_hints}"

        # Get semantic search results for context

        search_results = perform_semantic_search(search_query, top_k=8)

        # Create context from search results

        semantic_context = ""

        if search_results:

            semantic_context = "Relevant information from vector search:\n\n"

            for i, doc in enumerate(search_results):

                semantic_context += f"[{i+1}] {doc.page_content}\n\n"

        # Add key terms to context

        if any(key_terms.values()):

            semantic_context += "\nExtracted key terms from question:\n"

            if key_terms['months']:

                semantic_context += f"- Months detected: {key_terms['months']}\n"

            if key_terms['percentages']:

                semantic_context += f"- Percentages: {key_terms['percentages']}\n"

            if key_terms['comparisons']:

                semantic_context += f"- Comparison operators: {key_terms['comparisons']}\n"

            if key_terms['calculations']:

                semantic_context += f"- Calculation types: {key_terms['calculations']}\n"

        # Get comprehensive schema info with live distinct values

        schema_info = get_enhanced_schema_info()

        # Initialize variables for iterative process

        all_previous_data = []

        max_iterations = 5

        best_response = None

        best_confidence = 0

        best_sql = None

        for iteration in range(max_iterations):

            print(f"\n--- Iteration {iteration + 1} ---")

            # Get response from OpenAI

            openai_response = await prompt_openai_with_rag(
                schema_info,
                question,
                all_previous_data,
                semantic_context,
                fund_id=fund_id,
            )

            if "error" in openai_response:

                print(f"Error from OpenAI: {openai_response['error']}")

                if "raw_response" in openai_response:

                    print(f"Raw response: {openai_response['raw_response']}")

                break

            confidence = openai_response.get('confidence', 0)

            print(f"Confidence: {confidence}%")

            # Execute the SQL query

            sql_query = openai_response.get('sql_query')

            if sql_query:

                print(f"Generated SQL Query:\n{sql_query}")

                # Validate the query before execution

                validation_result = validate_query(sql_query, question)

                if not validation_result["valid"]:

                    print(f"Query validation failed!")

                    print(format_validation_report(validation_result))

                    # Treat validation failure as an error and retry

                    error_msg = f"Query validation failed: {'; '.join(validation_result['errors'])}. Suggestions: {'; '.join(validation_result['suggestions'])}"

                    iteration_data = {
                        "iteration": iteration + 1,
                        "query": sql_query,
                        "results": {
                            "error": error_msg
                        },
                        "confidence": confidence
                    }

                    all_previous_data.append(iteration_data)

                    continue

                elif validation_result["warnings"]:

                    print(f"Query has warnings:")

                    print(format_validation_report(validation_result))

                query_results = execute_query(sql_query)

                if isinstance(query_results,
                              dict) and "error" in query_results:

                    print(f"SQL Error: {query_results['error']}")

                    # Analyze the error for better recovery

                    error_analysis = _analyze_sql_error(
                        query_results['error'], sql_query, question)

                    enhanced_error_msg = f"{query_results['error']}. {error_analysis['suggestion']}"

                    # Store the error with enhanced guidance for next iteration

                    iteration_data = {
                        "iteration": iteration + 1,
                        "query": sql_query,
                        "results": {
                            "error": enhanced_error_msg
                        },
                        "confidence": confidence
                    }

                    all_previous_data.append(iteration_data)

                    continue

                # Check for empty results and analyze if it's likely an error

                if isinstance(query_results, list) and len(query_results) == 0:

                    analysis = _analyze_empty_result(sql_query, question)

                    if analysis["likely_error"]:

                        print(
                            f"Empty result detected - Likely error: {analysis['reason']}"
                        )

                        print(f"Suggestion: {analysis['suggestion']}")

                        # Treat as error and retry with guidance

                        error_msg = f"Query returned 0 rows but this is likely an error. {analysis['reason']}. Fix: {analysis['suggestion']}"

                        iteration_data = {
                            "iteration": iteration + 1,
                            "query": sql_query,
                            "results": {
                                "error": error_msg
                            },
                            "confidence": confidence
                        }

                        all_previous_data.append(iteration_data)

                        continue

                # Store this iteration's results

                iteration_data = {
                    "iteration": iteration + 1,
                    "query": sql_query,
                    "results": query_results,
                    "confidence": confidence
                }

                all_previous_data.append(iteration_data)

                # Update best response if this one has higher confidence

                if confidence > best_confidence:

                    best_confidence = confidence

                    best_response = {
                        "answer": openai_response["explanation"],
                        "query_results": query_results,
                        "confidence": confidence
                    }

                    best_sql = sql_query

                # Check if we have a satisfactory answer

                if openai_response.get('has_answer',
                                       False) and confidence >= 90:

                    print(f"Final answer found with {confidence}% confidence!")

                    return _build_multiple_module_response(
                        openai_response["explanation"], sql_query,
                        query_results, question)

            # If the model could not formulate a confident answer at all, return a clarification request

            if iteration == 0 and (not sql_query or confidence < 60):

                clarification_text = openai_response.get(
                    "explanation"
                ) or "Could you clarify the date scope and metric you want?"

                return _build_clarification_response(clarification_text)

            # Break if we've hit max iterations

            if iteration == max_iterations - 1:

                print(f"\nReached maximum iterations ({max_iterations}).")

                if best_response:

                    print("Returning best response found.")

                    return _build_multiple_module_response(
                        best_response["answer"], best_sql or "",
                        best_response["query_results"], question)

        # If we get here without a good answer, return the best we have or an error

        if best_response:

            return _build_multiple_module_response(
                best_response["answer"], best_sql or "",
                best_response["query_results"], question)

        else:

            response = {
                "response": {
                    "text":
                    "Could not process your question. Please try rephrasing or providing more details.",
                    "username": "admin"
                }
            }
            
            # Round all numbers in the response to 2 decimal places
            return _round_numbers_in_response(response)

    except Exception as e:

        print(f"Error answering question: {str(e)}")

        response = {
            "response": {
                "text": f"Error processing your question: {str(e)}",
                "username": "admin"
            }
        }
        
        # Round all numbers in the response to 2 decimal places
        return _round_numbers_in_response(response)
