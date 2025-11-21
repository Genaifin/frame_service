import re
from typing import Dict, List


def validate_trial_balance_query(sql: str) -> Dict[str, any]:
    """
    Validate queries against trial_balance table for common mistakes
    
    Returns: {
        "valid": bool,
        "warnings": List[str],
        "errors": List[str],
        "suggestions": List[str]
    }
    """
    issues = {
        "valid": True,
        "warnings": [],
        "errors": [],
        "suggestions": []
    }
    
    if not sql:
        return issues
    
    sql_lower = sql.lower()
    
    # Check 1: Wrong plural form for type - CRITICAL ERROR
    if re.search(r"type\s*=\s*['\"]expenses['\"]", sql_lower):
        issues["errors"].append("Using 'Expenses' (plural) - should be 'Expense' (singular)")
        issues["suggestions"].append("Change to: type = 'Expense'")
        issues["valid"] = False
    
    if re.search(r"type\s*=\s*['\"]revenues['\"]", sql_lower):
        issues["errors"].append("Using 'Revenues' (plural) - should be 'Revenue' (singular)")
        issues["suggestions"].append("Change to: type = 'Revenue'")
        issues["valid"] = False
    
    if re.search(r"type\s*=\s*['\"]assets['\"]", sql_lower):
        # Note: 'Assets' is actually correct, but if lowercase 'assets' appears, might be a typo
        # Let's check if it's properly capitalized
        if re.search(r"type\s*=\s*['\"]assets['\"]", sql):  # lowercase check (not sql_lower)
            issues["warnings"].append("Type value 'assets' should be capitalized: 'Assets'")
            issues["suggestions"].append("Change to: type = 'Assets' (capital A)")
    
    # Check 2: Searching accounting_head for common expense names - CRITICAL ERROR
    expense_keywords = ["legal", "audit", "bank", "admin", "custodian", "management", "performance", "fee"]
    for keyword in expense_keywords:
        # Check if accounting_head is used with LIKE for expense names
        if re.search(rf"accounting_head\s+like\s+['\"]%{keyword}%['\"]", sql_lower):
            issues["errors"].append(f"Searching accounting_head for '{keyword}' - this column contains technical codes, not account names")
            issues["suggestions"].append(f"Use financial_account instead: financial_account LIKE '%{keyword.title()}%'")
            issues["valid"] = False
    
    # Check 3: Missing date join when file_date is referenced
    if "file_date" in sql_lower and "trial_balance" in sql_lower:
        if "navpack_version" not in sql_lower or "nav_pack" not in sql_lower:
            issues["errors"].append("Date filtering (file_date) without proper joins through navpack_version and nav_pack")
            issues["suggestions"].append("Add: JOIN navpack_version nv ON tb.navpack_version_id = nv.navpack_version_id JOIN nav_pack np ON nv.navpack_id = np.navpack_id")
            issues["valid"] = False
    
    # Check 4: Using invalid type values
    # Extract type values from the query
    type_pattern = r"type\s*=\s*['\"](\w+)['\"]"
    type_matches = re.findall(type_pattern, sql, re.IGNORECASE)
    valid_types = ["Assets", "Liabilities", "Revenue", "Expense", "Capital"]
    for type_val in type_matches:
        if type_val not in valid_types:
            issues["errors"].append(f"Invalid type value '{type_val}' - must be one of: {', '.join(valid_types)}")
            issues["suggestions"].append(f"Use exact capitalization: {', '.join(valid_types)}")
            issues["valid"] = False
    
    # Check 5: Warning if category/accounting_head used in WHERE without NULL handling
    if re.search(r"where.*category\s*=", sql_lower):
        if "coalesce" not in sql_lower and "is null" not in sql_lower and "is not null" not in sql_lower:
            issues["warnings"].append("Filtering by category without NULL handling (30% of rows have NULL category)")
            issues["suggestions"].append("Consider: WHERE (category = '...' OR category IS NULL) or use COALESCE")
    
    if re.search(r"where.*accounting_head\s*=", sql_lower):
        if "coalesce" not in sql_lower and "is null" not in sql_lower and "is not null" not in sql_lower:
            issues["warnings"].append("Filtering by accounting_head without NULL handling (20% of rows have NULL accounting_head)")
            issues["suggestions"].append("Consider: WHERE (accounting_head = '...' OR accounting_head IS NULL) or use COALESCE")
    
    return issues


def validate_portfolio_query(sql: str) -> Dict[str, any]:
    """
    Validate queries against portfolio_valuation table
    
    Returns: {
        "valid": bool,
        "warnings": List[str],
        "errors": List[str],
        "suggestions": List[str]
    }
    """
    issues = {
        "valid": True,
        "warnings": [],
        "errors": [],
        "suggestions": []
    }
    
    if not sql:
        return issues
    
    sql_lower = sql.lower()
    
    # Check 1: Missing date join when file_date is referenced
    if "file_date" in sql_lower and "portfolio_valuation" in sql_lower:
        if "navpack_version" not in sql_lower or "nav_pack" not in sql_lower:
            issues["errors"].append("Date filtering without proper joins through navpack_version and nav_pack")
            issues["suggestions"].append("Add: JOIN navpack_version nv ON pv.navpack_version_id = nv.navpack_version_id JOIN nav_pack np ON nv.navpack_id = np.navpack_id")
            issues["valid"] = False
    
    # Check 2: Invalid inv_type values
    inv_type_pattern = r"inv_type\s*=\s*['\"](\w+)['\"]"
    inv_type_matches = re.findall(inv_type_pattern, sql, re.IGNORECASE)
    valid_inv_types = ["CASH", "EQUITY", "BOND", "DERIVATIVE", "FUND", "OTHER"]
    for inv_type in inv_type_matches:
        if inv_type.upper() not in valid_inv_types:
            issues["warnings"].append(f"inv_type value '{inv_type}' may not be valid - common values are: {', '.join(valid_inv_types)}")
    
    return issues


def validate_query(sql: str, question: str = "") -> Dict[str, any]:
    """
    Main validation function - determines which validators to run based on query content
    
    Returns: {
        "valid": bool,
        "warnings": List[str],
        "errors": List[str],
        "suggestions": List[str]
    }
    """
    combined_issues = {
        "valid": True,
        "warnings": [],
        "errors": [],
        "suggestions": []
    }
    
    if not sql:
        combined_issues["valid"] = False
        combined_issues["errors"].append("No SQL query provided")
        return combined_issues
    
    sql_lower = sql.lower()
    question_lower = question.lower() if question else ""
    
    # Check for "all data" queries that incorrectly filter for only latest month
    if question_lower:
        wants_all_data = any(phrase in question_lower for phrase in [
            "all data", "all months", "all periods", "for all", "across all"
        ])
        wants_comparison = any(phrase in question_lower for phrase in [
            "month over month", "month-over-month", "prior month", "previous month",
            "exceeded", "increased", "decreased", "changed"
        ])
        
        if wants_all_data and wants_comparison:
            # Check if query filters for a specific date (likely only checking latest month)
            if re.search(r"file_date\s*=\s*['\"]?\d{4}-\d{2}-\d{2}['\"]?", sql_lower):
                # Check if it's not in a subquery for getting previous month
                if "lag(" not in sql_lower and "lead(" not in sql_lower:
                    combined_issues["warnings"].append(
                        "Query asks for 'all data' month-over-month comparison but filters for specific date. "
                        "This will only return one month instead of all months meeting criteria."
                    )
                    combined_issues["suggestions"].append(
                        "Use window functions (LAG/LEAD) to compare ALL consecutive months, not just latest. "
                        "Remove specific date filter and return all months where condition is met."
                    )
    
    # Run appropriate validators based on tables involved
    if "trial_balance" in sql_lower:
        tb_issues = validate_trial_balance_query(sql)
        combined_issues["warnings"].extend(tb_issues["warnings"])
        combined_issues["errors"].extend(tb_issues["errors"])
        combined_issues["suggestions"].extend(tb_issues["suggestions"])
        if not tb_issues["valid"]:
            combined_issues["valid"] = False
    
    if "portfolio_valuation" in sql_lower:
        pv_issues = validate_portfolio_query(sql)
        combined_issues["warnings"].extend(pv_issues["warnings"])
        combined_issues["errors"].extend(pv_issues["errors"])
        combined_issues["suggestions"].extend(pv_issues["suggestions"])
        if not pv_issues["valid"]:
            combined_issues["valid"] = False
    
    # General checks for all queries
    
    # Check 1: Schema prefix usage
    tables = ["trial_balance", "portfolio_valuation", "dividend", "nav_pack", "navpack_version", "source", "kpi_library", "kpi_thresholds"]
    for table in tables:
        # Check if table is used without schema prefix
        if re.search(rf"\bfrom\s+{table}\b", sql_lower) or re.search(rf"\bjoin\s+{table}\b", sql_lower):
            if not re.search(rf"\b(nexbridge|public)\.{table}\b", sql_lower):
                combined_issues["warnings"].append(f"Table '{table}' used without schema prefix - should be 'nexbridge.{table}'")
    
    # Check 2: SQL injection risks (basic check)
    dangerous_patterns = [r";\s*drop\s+table", r";\s*delete\s+from", r";\s*truncate", r"--.*drop", r"--.*delete"]
    for pattern in dangerous_patterns:
        if re.search(pattern, sql_lower):
            combined_issues["errors"].append("Query contains potentially dangerous SQL patterns")
            combined_issues["valid"] = False
            break
    
    return combined_issues


def format_validation_report(issues: Dict[str, any]) -> str:
    """Format validation issues into a readable report"""
    if issues["valid"] and not issues["warnings"]:
        return "Query validation passed"
    
    report = []
    
    if issues["errors"]:
        report.append("ERRORS:")
        for error in issues["errors"]:
            report.append(f"  - {error}")
    
    if issues["suggestions"]:
        report.append("\nSUGGESTIONS:")
        for suggestion in issues["suggestions"]:
            report.append(f"  - {suggestion}")
    
    if issues["warnings"]:
        report.append("\nWARNINGS:")
        for warning in issues["warnings"]:
            report.append(f"  - {warning}")
    
    return "\n".join(report)
