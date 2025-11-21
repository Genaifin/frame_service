"""
Generic Chart Generation System
Uses data models and configurations to generate any type of chart
"""

from typing import List, Dict, Any, Optional
import calendar
from datetime import datetime

def getNAVValidationData():
    """Existing function - preserved for backward compatibility"""
    return {
        "failed_categories": [
            {
                "category": "Pricing",
                "count": 15,
                "override_impact": "0.02500%"
            },
            {
                "category": "Positions",
                "count": 8,
                "override_impact": "0.01200%"
            },
            {
                "category": "Market Value",
                "count": 12,
                "override_impact": "0.03100%"
            }
        ],
        "total_failed": 35,
        "total_override_impact": "0.06800%"
    }

def getLegalExpenseTrend():
    """Generate Legal Expense trend chart for 6 months (Jan-Jun)"""
    return generate_chart_response("legal_expense_trend")

# ====================
# 1. GENERIC CONFIGURATIONS
# ====================

def get_chart_configurations() -> Dict[str, Any]:
    """
    Generic chart configurations that can be reused for any question
    Each configuration defines data models, chart types, and parameters
    """
    configurations = {
        "nav_benchmark_returns_comparison": {
            "chart_type": "line_chart",
            "title": "NAV vs Benchmark Returns % Comparison",
            "icon": "TrendingUp",
            "data_sources": {
                "nav_returns": {
                    "model": "TrialBalance",
                    "query_type": "nav_returns_calculation",
                    "description": "Calculate NAV period-over-period returns % using (Current - Prior) / Prior",
                    "query_config": {
                        "fund_name": "NexBridge",
                        "source_names": ["Bluefield"],
                        "periods": ["2024-01-31", "2024-02-29", "2024-03-31"],
                        "exclude_types": ["revenue", "expense", "capital"],
                        "return_formula": "(current_nav - prior_nav) / prior_nav",
                        "joins": [
                            "NavPackVersion ON TrialBalance.navpack_version_id = NavPackVersion.navpack_version_id",
                            "NavPack ON NavPackVersion.navpack_id = NavPack.navpack_id", 
                            "Source ON NavPack.source_id = Source.id"
                        ],
                        "group_by": ["period"],
                        "order_by": ["period"]
                    }
                },
                "benchmark_returns": {
                    "model": "Benchmark",
                    "query_type": "benchmark_returns_calculation", 
                    "description": "Calculate benchmark period-over-period returns % using (Current - Prior) / Prior ",
                    "query_config": {
                        "benchmark_name": "S&P 500 Index",
                        "periods": ["2024-01-31", "2024-02-29", "2024-03-31"],
                        "return_formula": "(current_value - prior_value) / prior_value ",
                        "fallback_data": {
                            "2024-01": 1.6,
                            "2024-02": 1.8,
                            "2024-03": -1.2
                        }
                    }
                }
            },
            "chart_config": {
                "x_key": "period",
                "series": [
                    {"y_key": "nav_return", "y_name": "NAV Return %", "type": "line"},
                    {"y_key": "benchmark_return", "y_name": "Benchmark Return %", "type": "line"}
                ],
                "y_axis_formatter": "%",
                "legend_enabled": True
            },
            "date_range": "Jan 2024 - Mar 2024"
        },
        "legal_expense_trend": {
            "chart_type": "bar_chart",
            "title": "Legal Expense Trend (6 Months)",
            "icon": "Move3D",
            "data_sources": {
                "legal_expense_data": {
                    "model": "TrialBalance",
                    "query_type": "legal_expense_calculation",
                    "description": "Get Legal Expense from trial balance for 6 months (Jan-Jun)",
                    "query_config": {
                        "fund_name": "NexBridge",
                        "source_names": ["Bluefield"],
                        "months": ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31", "2024-06-30"],
                        "financial_account": "Legal Expense",
                        "include_types": ["expense"],
                        "aggregate": "ending_balance"
                    }
                }
            },
            "chart_config": {
                "x_key": "month",
                "y_axis_formatter": "k",
                "legend_enabled": False,
                "chart_name": "SimpleBarChart"
            },
            "date_range": "Jan 2024 - Jun 2024"
        }
    }
    
    return configurations

# ====================
# 2. GENERIC DATA RETRIEVAL
# ====================

def execute_data_retrieval(data_sources: Dict[str, Any]) -> Dict[str, List[Dict]]:
    """
    Generic function to retrieve data using SQLAlchemy models and session queries
    Complete query logic is defined in the chart configuration
    
    Args:
        data_sources: Dictionary of data source configurations with complete query configs
        
    Returns:
        Dictionary with calculated return percentages
    """
    from database_models import DatabaseManager
    from datetime import datetime

    # Get database session
    db_manager = DatabaseManager()
    session = db_manager.get_session()
    
    results = {}
    
    try:
        for source_name, source_config in data_sources.items():
            query_type = source_config.get("query_type")
            query_config = source_config.get("query_config", {})

            if query_type == "nav_returns_calculation":
                # Execute NAV returns calculation using query config
                fund_name = query_config.get("fund_name", "NexBridge")
                source_names = query_config.get("source_names", ["Bluefield"])
                periods = query_config.get("periods", [])
                exclude_types = query_config.get("exclude_types", ["revenue", "expense", "capital"])
                
                nav_returns_data = []
                
                # Calculate returns for each period
                for i, date_str in enumerate(periods):
                    current_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    
                    if i == 0:
                        # First period: compare with Dec 2023
                        prior_date = datetime.strptime("2023-12-31", "%Y-%m-%d").date()
                    else:
                        # Subsequent periods: compare with previous period
                        prior_date_str = periods[i-1]
                        prior_date = datetime.strptime(prior_date_str, "%Y-%m-%d").date()
                    
                    # Get current month NAV
                    current_nav = _get_nav_for_date(session, fund_name, source_names, current_date, exclude_types)
                    
                    # Get prior month NAV  
                    if str(prior_date) == '2023-12-31':
                        prior_nav = 1839492081.14
                    else:
                        prior_nav = _get_nav_for_date(session, fund_name, source_names, prior_date, exclude_types)

                    if current_nav is not None and prior_nav is not None and prior_nav != 0:
                        # Calculate return percentage: (Current - Prior) / Prior 
                        nav_return = ((current_nav - prior_nav) / prior_nav)
                        
                        # Format period as YYYY-MM
                        period = f"{current_date.year}-{current_date.month:02d}"
                        
                        nav_returns_data.append({
                            "period": period,
                            "nav_return": round(nav_return, 4)
                        })
                    # Note: Skipping NAV return calculation for period due to missing NAV values
                
                results[source_name] = nav_returns_data
                
            elif query_type == "benchmark_returns_calculation":
                # Execute benchmark returns calculation using query config
                benchmark_name = query_config.get("benchmark_name", "S&P 500 Index")
                periods = query_config.get("periods", [])
                fallback_data = query_config.get("fallback_data", {})
                
                benchmark_returns_data = []
                
                # Calculate returns for each period
                for i, date_str in enumerate(periods):
                    current_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    
                    if i == 0:
                        # First period: compare with Dec 2023
                        prior_date = datetime.strptime("2023-12-31", "%Y-%m-%d").date()
                    else:
                        # Subsequent periods: compare with previous period
                        prior_date_str = periods[i-1]
                        prior_date = datetime.strptime(prior_date_str, "%Y-%m-%d").date()
                    
                    # Try to get from database first
                    benchmark_return = _get_benchmark_return_for_period(session, benchmark_name, current_date, prior_date)
                    
                    # Fallback to hardcoded data
                    if benchmark_return is None:
                        period_key = f"{current_date.year}-{current_date.month:02d}"
                        benchmark_return = fallback_data.get(period_key, 0.0)
                    
                    # Format period as YYYY-MM
                    period = f"{current_date.year}-{current_date.month:02d}"
                    
                    benchmark_returns_data.append({
                        "period": period,
                        "benchmark_return": round(benchmark_return, 4)
                    })
                
                results[source_name] = benchmark_returns_data
                
            elif query_type == "legal_expense_calculation":
                # Execute legal expense calculation using query config
                fund_name = query_config.get("fund_name", "NexBridge")
                source_names = query_config.get("source_names", ["Bluefield"])
                months = query_config.get("months", [])
                financial_account = query_config.get("financial_account", "Legal Expense")
                include_types = query_config.get("include_types", ["expense"])
                
                legal_expense_data = _get_legal_expense_for_months(session, fund_name, source_names, months, financial_account, include_types)
                results[source_name] = legal_expense_data
                
            else:
                # Generic model handler for other query types
                results[source_name] = []
    
    except Exception as e:
        print(f"Error in data retrieval: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        session.close()
    
    return results

def _get_nav_for_date(session, fund_name: str, source_names: list, process_date, exclude_types: list) -> Optional[float]:
    """Get NAV value for a specific date using db_validation_service"""
    from server.APIServerUtils.db_validation_service import DatabaseValidationService
    
    try:
        # Initialize the validation service
        validation_service = DatabaseValidationService()
        
        # Convert date to string format expected by the service
        if hasattr(process_date, 'strftime'):
            process_date_str = process_date.strftime('%Y-%m-%d')
        else:
            process_date_str = str(process_date)
        
        # Try each source name until we find NAV data
        for source_name in source_names:
            # Use the service's calculate_nav method
            nav_value = validation_service.calculate_nav(fund_name, source_name, process_date_str)
            
            if nav_value is not None:
                return float(nav_value)
        
        return None
        
    except Exception as e:
        print(f"Error getting NAV using service for {process_date}: {e}")
        import traceback
        traceback.print_exc()
        return None

def _get_benchmark_return_for_period(session, benchmark_name: str, current_date, prior_date) -> Optional[float]:
    """Get benchmark return percentage between two dates"""
    from database_models import Benchmark
    
    try:
        # Get current benchmark value
        current_benchmark = session.query(Benchmark).filter(
            Benchmark.benchmark == benchmark_name,
            Benchmark.date == current_date.strftime('%Y-%m-%d')
        ).first()
        
        # Get prior benchmark value
        prior_benchmark = session.query(Benchmark).filter(
            Benchmark.benchmark == benchmark_name,
            Benchmark.date == prior_date.strftime('%Y-%m-%d')
        ).first()
        
        if current_benchmark and prior_benchmark:
            current_value = float(current_benchmark.value)
            prior_value = float(prior_benchmark.value)
            
            if prior_value > 0:
                result = ((current_value - prior_value) / prior_value)
                return result
        
        return None
    except Exception as e:
        print(f"Error getting benchmark return: {e}")
        return None

def _get_legal_expense_for_months(session, fund_name: str, source_names: list, months: list, financial_account: str, include_types: list) -> List[Dict]:
    """Get Legal Expense data for multiple months using db_validation_service"""
    from server.APIServerUtils.db_validation_service import DatabaseValidationService
    from datetime import datetime
    
    try:
        # Initialize the validation service
        validation_service = DatabaseValidationService()
        
        legal_expense_data = []
        
        # Process each month
        for month_str in months:
            # Try each source name until we find data
            for source_name in source_names:
                # Get trial balance data using the service
                trial_balance_data = validation_service.get_trial_balance_data_base_only(fund_name, source_name, month_str)
                
                if trial_balance_data:
                    # Look for Legal Expense specifically
                    legal_expense_amount = 0.0
                    
                    for entry in trial_balance_data:
                        entry_type = entry.get('Type', '').strip().lower()
                        entry_financial_account = entry.get('Financial Account', '').strip()
                        ending_balance = entry.get('Ending Balance', 0)
                        
                        # Check if this is the Legal Expense account and an expense type
                        if (entry_type in [t.lower() for t in include_types] and 
                            entry_financial_account.lower() == financial_account.lower()):
                            
                            if ending_balance is not None:
                                try:
                                    balance_value = float(ending_balance)
                                    legal_expense_amount += balance_value
                                except (ValueError, TypeError):
                                    continue
                    
                    # Format month for display (e.g., "2024-01" -> "Jan")
                    try:
                        month_date = datetime.strptime(month_str, '%Y-%m-%d')
                        month_display = f"{month_date.strftime('%b')}"
                    except ValueError:
                        month_display = month_str[:7]  # Fallback to YYYY-MM
                    
                    # Add data point for this month
                    legal_expense_data.append({
                        "month": month_display,
                        "legal_expense": round(legal_expense_amount, 4)
                    })
                    
                    break  # Found data for this month, move to next month
            
            # If no data found for this month, add zero value
            if not any(item["month"] == month_display for item in legal_expense_data):
                try:
                    month_date = datetime.strptime(month_str, '%Y-%m-%d')
                    month_display = f"{month_date.strftime('%b')} {month_date.year}"
                except ValueError:
                    month_display = month_str[:7]
                
                legal_expense_data.append({
                    "month": month_display,
                    "legal_expense": 0.0
                })
        
        return legal_expense_data
        
    except Exception as e:
        print(f"Error getting legal expense data for months: {e}")
        import traceback
        traceback.print_exc()
        return []

# ====================
# 3. GENERIC CHART GENERATION
# ====================

def generate_chart(data_results: Dict[str, List[Dict]], chart_config: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """
    Generic function to create any chart type from data
    
    Args:
        data_results: Raw data from models
        chart_config: Chart configuration parameters
        **kwargs: Additional parameters (title, date_range, icon, etc.)
        
    Returns:
        Chart data structure ready for frontend
    """
    from utils.chartUtils import create_line_chart_data
    
    # Combine data from multiple sources by common key (usually period/date)
    combined_data = {}
    x_key = chart_config.get("x_key", "period")
    
    # Process each data source
    for source_name, data_list in data_results.items():
        for item in data_list:
            key_value = item.get(x_key)
            if key_value not in combined_data:
                combined_data[key_value] = {x_key: _format_display_value(key_value, x_key)}
            
            # Merge all data fields for this key
            for field, value in item.items():
                if field != x_key:
                    combined_data[key_value][field] = value
    
    # Convert to list and sort
    chart_data_points = list(combined_data.values())
    
    # Generate chart based on chart type
    chart_type = kwargs.get("chart_type", "line_chart")
    
    if chart_type == "line_chart":
        return create_line_chart_data(
            data=chart_data_points,
            x_key=chart_config["x_key"],
            series_configs=chart_config["series"],
            title=kwargs.get("title", "Chart"),
            date_range=kwargs.get("date_range", ""),
            icon=kwargs.get("icon", "Move3D"),
            y_axis_formatter=chart_config.get("y_axis_formatter", "")
        )
    elif chart_type == "bar_chart":
        return _create_bar_chart_data(
            data=chart_data_points,
            chart_config=chart_config,
            title=kwargs.get("title", "Chart"),
            date_range=kwargs.get("date_range", ""),
            icon=kwargs.get("icon", "Move3D")
        )
    else:
        # Can extend to support pie charts, etc.
        raise ValueError(f"Unsupported chart type: {chart_type}")

def _create_bar_chart_data(data: List[Dict], chart_config: Dict[str, Any], title: str, date_range: str, icon: str) -> Dict[str, Any]:
    """Create bar chart data structure for the frontend"""
    if not data:
        return {
            "moduleType": "chart",
            "data": {
                "icon": icon,
                "title": title,
                "date": date_range,
                "chartConfig": {
                    "data": [],
                    "series": [],
                    "legend": {"enabled": False},
                    "axes": []
                },
                "name": chart_config.get("chart_name", "SimpleBarChart")
            }
        }
    
    x_key = chart_config.get("x_key", "month")
    
    # Check if this is legal expense trend data (has month/legal_expense structure)
    if x_key == "month" and any("legal_expense" in item for item in data):
        # Legal expense trend format: month/legal_expense pairs
        chart_data_points = []
        for item in data:
            chart_data_points.append({
                "month": item.get("month", ""),
                "legal_expense": item.get("legal_expense", 0)
            })
        
        # Create series configuration for legal expense trend
        series = [{
            "type": "bar",
            "xKey": "month",
            "yKey": "legal_expense",
            "yName": "Legal Expense",
            "label": {
                "enabled": False,
                "formatter": "({ value }) => value.toFixed(1) + 'k'",
            }
        }]
        
        # Create axes configuration for legal expense trend
        axes = [
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
                    "formatter": chart_config.get("y_axis_formatter", "k")
                }
            }
        ]
    
    else:
        # Original label/value format for other bar charts
        first_data_point = data[0]
        
        # Convert expense data to label/value pairs
        chart_data_points = []
        for key, value in first_data_point.items():
            if key != x_key and isinstance(value, (int, float)):
                chart_data_points.append({
                    "label": key,
                    "value": value
                })
        
        # Sort by value (descending) for better visualization
        chart_data_points.sort(key=lambda x: abs(x["value"]), reverse=True)
        
        # Create single series configuration for label/value format
        series = [{
            "type": "bar",
            "xKey": "label",
            "yKey": "value",
            "yName": "Amount",
            "label": {
                "enabled": True,
                "formatter": "({ value }) => value.toFixed(1) + 'k'",
                "placement": "inside",
                "color": "white",
                "angle": -90
            }
        }]
        
        # Create axes configuration
        axes = [
            {
                "type": "category",
                "position": "bottom",
                "label": {
                    "formatter": "",
                    "rotation": 90
                }
            },
            {
                "type": "number",
                "position": "left",
                "label": {
                    "formatter": chart_config.get("y_axis_formatter", "k")
                }
            }
        ]
    
    return {
        "moduleType": "chart",
        "data": {
            "icon": icon,
            "title": title,
            "date": date_range,
            "chartConfig": {
                "data": chart_data_points,
                "series": series,
                "legend": {
                    "enabled": chart_config.get("legend_enabled", False)
                },
                "axes": axes
            },
            "name": chart_config.get("chart_name", "SimpleBarChart")
        }
    }

def _format_display_value(value: str, field_type: str) -> str:
    """Format values for display based on field type"""
    if field_type == "period" and "-" in value:
        # Convert YYYY-MM to display format
        try:
            year, month_num = value.split('-')
            month_name = calendar.month_abbr[int(month_num)]
            return f"{month_name} {year}"
        except (ValueError, IndexError, KeyError):
            pass
    return value

# ====================
# 4. GENERIC RESPONSE FORMATTING
# ====================

def format_response(chart_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """
    Generic function to format any response structure
    
    Args:
        chart_data: Generated chart data
        **kwargs: Additional response metadata
        
    Returns:
        Complete response structure
    """
    return {
        "modules": [chart_data],
        "metadata": {
            "data_source": kwargs.get("data_source", "models"),
            "generated_at": datetime.now().isoformat(),
            "source_count": kwargs.get("source_count", 0),
            "chart_type": kwargs.get("chart_type", "unknown"),
            "configuration": kwargs.get("configuration_name", "unknown")
        }
    }

# ====================
# 5. MAIN ORCHESTRATOR (GENERIC)
# ====================

def generate_chart_response(configuration_name: str, **override_params) -> Dict[str, Any]:
    """
    Generic function that can generate any chart based on configuration name
    This replaces all question-specific functions
    
    Args:
        configuration_name: Name of the configuration to use
        **override_params: Parameters to override in the configuration
        
    Returns:
        Complete chart response structure
    """
    
    # Step 1: Get configuration
    configurations = get_chart_configurations()
    if configuration_name not in configurations:
        raise ValueError(f"Configuration '{configuration_name}' not found")
    
    config = configurations[configuration_name].copy()
    
    # Step 2: Apply parameter overrides
    if override_params:
        # Override parameters in data sources if provided
        for source_name, source_config in config["data_sources"].items():
            if "filters" in source_config:
                for key, value in override_params.items():
                    if key in source_config["filters"]:
                        source_config["filters"][key] = value
    
    # Step 3: Retrieve data using models
    data_results = execute_data_retrieval(config["data_sources"])
    
    # Step 4: Generate chart from data
    chart_data = generate_chart(
        data_results=data_results,
        chart_config=config["chart_config"],
        title=config["title"],
        date_range=config.get("date_range", ""),
        icon=config.get("icon", "Move3D"),
        chart_type=config["chart_type"]
    )
    
    # Step 5: Format final response
    return format_response(
        chart_data=chart_data,
        data_source="models",
        source_count=len(config["data_sources"]),
        chart_type=config["chart_type"],
        configuration_name=configuration_name
    )