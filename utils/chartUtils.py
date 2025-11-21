"""
Chart utilities for creating chart data structures
Generic functions to convert raw data into chart format
"""

from typing import List, Dict, Any
from datetime import datetime

def create_line_chart_data(
    data: List[Dict],
    x_key: str,
    series_configs: List[Dict],
    title: str = "Line Chart",
    date_range: str = "",
    icon: str = "Move3D",
    y_axis_formatter: str = "%"
) -> Dict[str, Any]:
    """
    Create a line chart data structure from raw data
    
    Args:
        data: List of dictionaries containing the chart data points
        x_key: The field name to use for x-axis values
        series_configs: List of series configurations, each containing:
            - y_key: Field name for y-axis values
            - y_name: Display name for the series
            - type: Chart type (default: "line")
        title: Chart title
        date_range: Date range string for display
        icon: Icon name for the chart
        y_axis_formatter: Formatter for y-axis labels (e.g., "%", "$", "")
    
    Returns:
        Dictionary with moduleType "chart" and formatted chart data
    """
    
    # Build series configuration
    series = []
    for config in series_configs:
        series.append({
            "type": config.get("type", "line"),
            "xKey": x_key,
            "yKey": config["y_key"],
            "yName": config["y_name"]
        })
    
    # Create the chart structure
    chart_data = {
        "moduleType": "chart",
        "data": {
            "icon": icon,
            "title": title,
            "date": date_range,
            "chartConfig": {
                "data": data,
                "series": series,
                "legend": {
                    "enabled": True
                },
                "axes": [
                    {
                        "type": "category",
                        "position": "bottom",
                        "label": {
                            "formatter": ""
                        }
                    },
                    {
                        "type": "number",
                        "position": "left",
                        "label": {
                            "formatter": y_axis_formatter
                        }
                    }
                ]
            },
            "name": "LineChart"
        }
    }
    
    return chart_data

def format_nav_benchmark_chart_data(
    nav_data: List[Dict],
    benchmark_data: List[Dict],
    title: str = "NAV vs Benchmark Comparison",
    date_range: str = ""
) -> Dict[str, Any]:
    """
    Format NAV and benchmark data into line chart structure
    
    Args:
        nav_data: List of dictionaries with 'period' and 'nav_value' keys
        benchmark_data: List of dictionaries with 'period' and 'benchmark_value' keys
        title: Chart title
        date_range: Date range for display
    
    Returns:
        Formatted chart data structure
    """
    
    # Combine data by period
    combined_data = {}
    
    # Add NAV data
    for item in nav_data:
        period = item['period']
        combined_data[period] = combined_data.get(period, {})
        combined_data[period]['period'] = period
        combined_data[period]['nav'] = item['nav_value']
    
    # Add benchmark data
    for item in benchmark_data:
        period = item['period']
        combined_data[period] = combined_data.get(period, {})
        combined_data[period]['period'] = period
        combined_data[period]['benchmark'] = item['benchmark_value']
    
    # Convert to list and sort by period
    chart_data_points = list(combined_data.values())
    chart_data_points.sort(key=lambda x: x['period'])
    
    # Define series configurations
    series_configs = [
        {"y_key": "nav", "y_name": "NAV"},
        {"y_key": "benchmark", "y_name": "Benchmark"}
    ]
    
    return create_line_chart_data(
        data=chart_data_points,
        x_key="period",
        series_configs=series_configs,
        title=title,
        date_range=date_range,
        icon="TrendingUp",
        y_axis_formatter=""  # No formatter for absolute values
    )
