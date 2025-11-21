"""
Common utility functions for validation checks
Used across different KPI validation modules
"""

def should_show_asset_type(validation_category: str, sub_type2: str, is_dual_source: bool) -> bool:
    """
    Determine if Asset Type column should be shown based on validation category and source type
    
    Args:
        validation_category: Main validation category (e.g., 'Pricing', 'Positions', 'Market Value', 'Trading I&E')
        sub_type2: Specific validation type (e.g., 'Major Price Change', 'Major Dividends')
        is_dual_source: True for dual source comparison, False for single source
        
    Returns:
        bool: True if Asset Type should be shown, False otherwise
    """
    # Based on the provided table requirements
    
    if validation_category == 'Pricing':
        # All pricing validations show Asset Type for both single and dual source
        return True
        
    elif validation_category == 'Positions':
        # All position validations show Asset Type for both single and dual source
        return True
        
    elif validation_category == 'Trading I&E':
        # Trading I&E validations show Asset Type for dual source
        # For single source, only show for Material Interest Accruals
        if is_dual_source:
            return True
        else:
            # Check if this is Material Interest Accruals - show Asset Type for single source
            if sub_type2 == 'Material Interest Accruals':
                return True
            else:
                return False
            
    elif validation_category == 'Market Value':
        # All market value validations show Asset Type for both single and dual source
        return True
        
    else:
        # For other categories (Expenses, Fees, etc.), don't show Asset Type
        return False

def extract_asset_type(item_data: dict) -> str:
    """
    Extract Asset Type from portfolio item data
    
    Args:
        item_data: Portfolio item dictionary
        
    Returns:
        str: Asset Type value or '-' if not found
    """
    # Try different possible field names for Asset Type
    asset_type_fields = ['Inv Type', 'Investment Type', 'Asset Type', 'Type', 'inv_type', 'investment_type', 'asset_type']
    
    for field in asset_type_fields:
        if field in item_data and item_data[field]:
            return str(item_data[field]).strip()
    
    # If not found in main fields, try in extra_data
    if 'extra_data' in item_data and item_data['extra_data']:
        try:
            import json
            extra_data = json.loads(item_data['extra_data'])
            for field in asset_type_fields:
                if field in extra_data and extra_data[field]:
                    return str(extra_data[field]).strip()
        except (json.JSONDecodeError, TypeError):
            pass
    
    return '-'


def format_asset_type_display(asset_type: str) -> str:
    """
    Format asset type for display in NAV validation details
    
    Args:
        asset_type: Raw asset type string
        
    Returns:
        str: Formatted asset type for display
    """
    if not asset_type or asset_type == '-':
        return '-'
    
    # Convert to uppercase for case-insensitive comparison
    asset_type_upper = asset_type.upper().strip()
    
    # Map specific asset types to display names
    if asset_type_upper == 'CASH':
        return 'Cash'
    elif asset_type_upper == 'CASHF':
        return 'Forwards'
    
    # Return original value for all other asset types
    return asset_type

def greater_than_threshold_check(data_list, field, threshold, identifier_field='Inv Id'):
    """
    Check if values in a field are greater than a threshold
    
    Args:
        data_list: List of data items to check
        field: Field name to check
        threshold: Threshold value to compare against
        identifier_field: Field to use as identifier for tracking items
    
    Returns:
        tuple: (failed_items, passed_items, total_items)
    """
    failed_items = []
    passed_items = []
    
    for item in data_list:
        try:
            value = float(item.get(field, 0))
            identifier = item.get(identifier_field, '-')
            
            # Extract Asset Type from item data
            asset_type = extract_asset_type(item)
            
            if value > threshold:
                failed_items.append({
                    'identifier': identifier,
                    'asset_type': asset_type,
                    'field': field,
                    'value': value,
                    'threshold': threshold,
                    'comparison': 'greater_than',
                    'raw_data': item
                })
            else:
                passed_items.append({
                    'identifier': identifier,
                    'asset_type': asset_type,
                    'field': field,
                    'value': value,
                    'threshold': threshold,
                    'comparison': 'greater_than',
                    'raw_data': item
                })
        except (ValueError, TypeError):
            # Handle invalid values
            asset_type = extract_asset_type(item)
            failed_items.append({
                'identifier': item.get(identifier_field, '-'),
                'asset_type': asset_type,
                'field': field,
                'value': item.get(field, 'Invalid'),
                'threshold': threshold,
                'comparison': 'greater_than',
                'error': 'Invalid numeric value',
                'raw_data': item
            })
    
    return failed_items, passed_items, len(data_list)


def less_than_threshold_check(data_list, field, threshold, identifier_field='Inv Id'):
    """
    Check if values in a field are less than a threshold
    
    Args:
        data_list: List of data items to check
        field: Field name to check
        threshold: Threshold value to compare against
        identifier_field: Field to use as identifier for tracking items
    
    Returns:
        tuple: (failed_items, passed_items, total_items)
    """
    failed_items = []
    passed_items = []
    
    for item in data_list:
        try:
            value = float(item.get(field, 0))
            identifier = item.get(identifier_field, '-')
            
            if value < threshold:
                failed_items.append({
                    'identifier': identifier,
                    'field': field,
                    'value': value,
                    'threshold': threshold,
                    'comparison': 'less_than',
                    'raw_data': item
                })
            else:
                passed_items.append({
                    'identifier': identifier,
                    'field': field,
                    'value': value,
                    'threshold': threshold,
                    'comparison': 'less_than',
                    'raw_data': item
                })
        except (ValueError, TypeError):
            # Handle invalid values
            failed_items.append({
                'identifier': item.get(identifier_field, '-'),
                'field': field,
                'value': item.get(field, 'Invalid'),
                'threshold': threshold,
                'comparison': 'less_than',
                'error': 'Invalid numeric value',
                'raw_data': item
            })
    
    return failed_items, passed_items, len(data_list)


def _create_composite_identifier(item, identifier_field='Inv Id'):
    """
    Create a composite identifier using (inv_id, description) for pricing validations
    
    Args:
        item: Portfolio data item
        identifier_field: Field to use as primary identifier
        
    Returns:
        tuple: (inv_id, description) as composite key
    """
    inv_id = item.get(identifier_field, '-')
    description = None
    
    # Extract description from extra_data
    if 'extra_data' in item and item['extra_data']:
        try:
            import json
            extra_data = json.loads(item['extra_data'])
            if 'description' in extra_data:
                description = extra_data['description']
        except (json.JSONDecodeError, TypeError):
            pass
    
    return (inv_id, description)


def price_change_percentage_check_with_composite_id(data_list_a, data_list_b, field, threshold, identifier_field='Inv Id'):
    """
    Check for percentage change using composite (inv_id, description) matching
    Skip securities that don't have matching composite identifiers between periods
    
    Args:
        data_list_a: List of data items from period A
        data_list_b: List of data items from period B
        field: Field to check for changes
        threshold: Threshold percentage (as decimal, e.g. 0.05 for 5%)
        identifier_field: Field to use as primary identifier
        
    Returns:
        tuple: (failed_items, passed_items, total_matched_items)
    """
    failed_items = []
    passed_items = []
    
    # Create lookup for period A data using composite identifiers
    data_a_lookup = {}
    for item in data_list_a:
        composite_key = _create_composite_identifier(item, identifier_field)
        data_a_lookup[composite_key] = item
    
    matched_items = 0
    skipped_items = 0
    
    for item_b in data_list_b:
        composite_key = _create_composite_identifier(item_b, identifier_field)
        inv_id, description = composite_key
        
        # Skip if no matching composite identifier in period A
        item_a = data_a_lookup.get(composite_key)
        if item_a is None:
            skipped_items += 1
            continue
            
        matched_items += 1
        
        # Get values from both periods
        try:
            raw_value_a = item_a.get(field)
            raw_value_b = item_b.get(field)
            
            # Skip items where either value is None, empty, 'nan', 'null', 'none', or NaN
            if (raw_value_a is None or raw_value_b is None or
                raw_value_a == '' or raw_value_b == '' or
                str(raw_value_a).lower() in ['nan', 'null', 'none'] or
                str(raw_value_b).lower() in ['nan', 'null', 'none'] or
                (isinstance(raw_value_a, float) and raw_value_a != raw_value_a) or  # NaN check
                (isinstance(raw_value_b, float) and raw_value_b != raw_value_b)):   # NaN check
                continue
            
            value_a = float(raw_value_a)
            value_b = float(raw_value_b)
            
            # Calculate percentage change (absolute for threshold comparison)
            if value_a != 0:
                percentage_change = abs((value_b - value_a) / abs(value_a)) * 100
                # Calculate signed percentage change for display using formula (B-A)/|A|
                signed_percentage_change = ((value_b - value_a) / abs(value_a)) * 100
            else:
                percentage_change = 999999.99 if value_b != 0 else 0  # Use large number instead of inf
                signed_percentage_change = 100 if value_b > 0 else (-100 if value_b < 0 else 0)
            
            # Use description for display, fallback to inv_id
            display_identifier = description if description else inv_id
            
            # Extract Asset Type from item data
            asset_type = extract_asset_type(item_b)
            
            # Determine if validation failed (ensure JSON-serializable boolean)
            is_failed = bool(percentage_change > threshold)
            
            # Calculate tooltip (opposite precision type) with consistent 3 decimal places
            absolute_change = float(value_b) - float(value_a)
            tooltip_format = f"${absolute_change:,.3f}" if absolute_change >= 0 else f"-${abs(absolute_change):,.3f}"
            
            # Create comprehensive validation item
            validation_item = {
                'identifier': display_identifier,
                'inv_id': inv_id,
                'description': description,
                'asset_type': asset_type,
                'field': field,
                'value_a': value_a,
                'value_b': value_b,
                'change': signed_percentage_change,  # Frontend expects this field
                'change_value': percentage_change,
                'percentage_change': signed_percentage_change,
                'precision_type': 'PERCENTAGE',
                'threshold': threshold,
                'is_failed': is_failed,
                'threshold_exceeded': is_failed,
            'display_change': f"{signed_percentage_change:.3f}%",
            'tooltip_change': tooltip_format,  # Opposite precision type for tooltip
                'comparison': 'greater_than',
                'raw_data_a': item_a,
                'raw_data_b': item_b
            }
            
            if is_failed:
                failed_items.append(validation_item)
            else:
                passed_items.append(validation_item)
                
        except (ValueError, TypeError) as e:
            # Skip items with invalid numeric values
            continue
    
    return failed_items, passed_items, matched_items


def missing_price_null_check(data_list_b, identifier_field='Inv Id', data_list_a=None):
    """
    Check for missing prices based on NULL end_local_market_price with non-zero quantity
    
    Rule: If end_local_market_price is NULL/None AND end_qty != 0, flag as missing price exception
    Zero is not considered an exception anymore - only NULL values
    Only check ending period data (not start period as it was checked in prior month)
    
    Args:
        data_list_b: List of portfolio data items to check (ending period)
        identifier_field: Field to use as identifier for tracking items
        data_list_a: Optional list of portfolio data from period A (for value_a calculation)
    
    Returns:
        tuple: (failed_items, passed_items, total_items)
    """
    failed_items = []
    passed_items = []
    
    # Create lookup for period A data if provided
    data_a_lookup = {}
    if data_list_a:
        data_a_lookup = {item.get(identifier_field): item for item in data_list_a}
    
    for item in data_list_b:
        identifier = item.get(identifier_field, '-')
        
        # For portfolio validations, use Description from extra_data as display identifier
        display_identifier = identifier
        if 'extra_data' in item and item['extra_data']:
            try:
                import json
                extra_data = json.loads(item['extra_data'])
                if 'description' in extra_data:
                    display_identifier = extra_data['description']
            except (json.JSONDecodeError, TypeError):
                pass
        
        # Get period A data for this identifier
        item_a = data_a_lookup.get(identifier) if data_a_lookup else None
        value_a = None  # Default - use None to indicate missing data
        if item_a:
            try:
                # Use End Local Market Price for consistency with what we're validating
                raw_price_a = item_a.get('End Local Market Price')
                if raw_price_a is not None:
                    value_a = float(raw_price_a)
            except (ValueError, TypeError):
                value_a = None
        
        # Get raw values for checking
        raw_price = item.get('End Local Market Price')
        raw_qty = item.get('End Qty', 0)
        
        # Check if price is NULL/None/missing
        is_price_null = (
            raw_price is None or 
            raw_price == '' or 
            str(raw_price).lower() in ['nan', 'null', 'none'] or
            (isinstance(raw_price, float) and raw_price != raw_price)  # NaN check
        )
        
        # Extract Asset Type from item data
        asset_type = extract_asset_type(item)
        
        try:
            end_qty = float(raw_qty)
            
            # Check if market price is NULL but quantity is not zero
            if is_price_null and end_qty != 0:
                failed_items.append({
                    'identifier': display_identifier,
                    'inv_id': identifier,  # Keep original for tracking
                    'asset_type': asset_type,
                    'end_local_market_price': raw_price,  # Use raw value to show NULL
                    'end_qty': end_qty,
                    'value_a': value_a,  # Actual period A market value
                    'value_b': raw_price,  # Period B price (NULL)
                    'issue': 'missing_price_null',
                    'raw_data': item
                })
            else:
                passed_items.append({
                    'identifier': display_identifier,
                    'inv_id': identifier,  # Keep original for tracking
                    'asset_type': asset_type,
                    'end_local_market_price': raw_price,
                    'end_qty': end_qty,
                    'value_a': value_a,  # Actual period A market value
                    'value_b': raw_price,  # Period B price
                    'issue': 'missing_price_null',
                    'raw_data': item
                })
        except (ValueError, TypeError):
            # Handle invalid quantity values - treat as failed if price is NULL
            if is_price_null:
                failed_items.append({
                    'identifier': display_identifier,
                    'inv_id': identifier,  # Keep original for tracking
                    'asset_type': asset_type,
                    'end_local_market_price': raw_price,
                    'end_qty': item.get('End Qty', 'Invalid'),
                    'value_a': value_a,  # Use calculated period A value
                    'value_b': raw_price,
                    'issue': 'missing_price_null',
                    'error': 'Invalid quantity value',
                    'raw_data': item
                })
            else:
                passed_items.append({
                    'identifier': display_identifier,
                    'inv_id': identifier,
                    'asset_type': asset_type,
                    'end_local_market_price': raw_price,
                    'end_qty': item.get('End Qty', 'Invalid'),
                    'value_a': value_a,
                    'value_b': raw_price,
                    'issue': 'missing_price_null',
                    'note': 'Invalid quantity but price not NULL',
                    'raw_data': item
                })
    
    return failed_items, passed_items, len(failed_items) + len(passed_items)


def missing_price_zero_mv_check(data_list_b, identifier_field='Inv Id', data_list_a=None):
    """
    Legacy function - Check for missing prices based on zero market value with non-zero quantity
    
    Rule: If end_local_mv = 0 AND end_qty != 0, flag as missing price exception
    This function is kept for backward compatibility but is deprecated
    Use missing_price_null_check instead for new NULL price validation logic
    
    Args:
        data_list_b: List of portfolio data items to check (ending period)
        identifier_field: Field to use as identifier for tracking items
        data_list_a: Optional list of portfolio data from period A (for value_a calculation)
    
    Returns:
        tuple: (failed_items, passed_items, total_items)
    """
    failed_items = []
    passed_items = []
    
    # Create lookup for period A data if provided
    data_a_lookup = {}
    if data_list_a:
        data_a_lookup = {item.get(identifier_field): item for item in data_list_a}
    
    for item in data_list_b:
        identifier = item.get(identifier_field, '-')
        
        # For portfolio validations, use Description from extra_data as display identifier
        display_identifier = identifier
        if 'extra_data' in item and item['extra_data']:
            try:
                import json
                extra_data = json.loads(item['extra_data'])
                if 'description' in extra_data:
                    display_identifier = extra_data['description']
            except (json.JSONDecodeError, TypeError):
                pass
        
        # Calculate period A value if available
        value_a = 0
        item_a = data_a_lookup.get(identifier)
        if item_a:
            try:
                value_a = float(item_a.get('End Local MV', 0))
            except (ValueError, TypeError):
                value_a = 0
        
        # Extract Asset Type from item data
        asset_type = extract_asset_type(item)
        
        try:
            end_local_mv = float(item.get('End Local MV', 0))
            end_qty = float(item.get('End Qty', 0))
            
            # Check if market value is zero but quantity is not zero
            if end_local_mv == 0 and end_qty != 0:
                failed_items.append({
                    'identifier': display_identifier,
                    'inv_id': identifier,
                    'asset_type': asset_type,
                    'end_local_mv': end_local_mv,
                    'end_qty': end_qty,
                    'value_a': value_a,
                    'value_b': end_local_mv,
                    'issue': 'missing_price',
                    'raw_data': item
                })
            else:
                passed_items.append({
                    'identifier': display_identifier,
                    'inv_id': identifier,
                    'asset_type': asset_type,
                    'end_local_mv': end_local_mv,
                    'end_qty': end_qty,
                    'value_a': value_a,
                    'value_b': end_local_mv,
                    'issue': 'missing_price',
                    'raw_data': item
                })
        except (ValueError, TypeError):
            # Handle invalid values - treat as failed
            failed_items.append({
                'identifier': display_identifier,
                'inv_id': identifier,
                'asset_type': asset_type,
                'end_local_mv': item.get('End Local MV', 'Invalid'),
                'end_qty': item.get('End Qty', 'Invalid'),
                'value_a': value_a,
                'value_b': item.get('End Local MV', 'Invalid'),
                'issue': 'missing_price',
                'raw_data': item
            })
    
    return failed_items, passed_items, len(failed_items) + len(passed_items)


def null_missing_check(data_list, field, identifier_field='Inv Id'):
    """
    Check for null or missing values in a field
    
    Args:
        data_list: List of data items to check
        field: Field name to check for null/missing values
        identifier_field: Field to use as identifier for tracking items
    
    Returns:
        tuple: (failed_items, passed_items, total_items)
    """
    failed_items = []
    passed_items = []
    
    for item in data_list:
        value = item.get(field)
        identifier = item.get(identifier_field, '-')
        
        # Check for null, None, empty string, or NaN values
        is_missing = (
            value is None or 
            value == '' or 
            str(value).lower() in ['nan', 'null', 'none'] or
            (isinstance(value, float) and value != value)  # NaN check
        )
        
        # Extract Asset Type from item data
        asset_type = extract_asset_type(item)
        
        if is_missing:
            failed_items.append({
                'identifier': identifier,
                'asset_type': asset_type,
                'field': field,
                'value': value,
                'issue': 'null_or_missing',
                'raw_data': item
            })
        else:
            passed_items.append({
                'identifier': identifier,
                'asset_type': asset_type,
                'field': field,
                'value': value,
                'issue': 'null_or_missing',
                'raw_data': item
            })
    
    return failed_items, passed_items, len(data_list)


def zero_quantity_check(data_list, field='End Qty', identifier_field='Inv Id'):
    """
    Check for zero quantities
    
    Args:
        data_list: List of data items to check
        field: Field name to check for zero values
        identifier_field: Field to use as identifier for tracking items
    
    Returns:
        tuple: (failed_items, passed_items, total_items)
    """
    failed_items = []
    passed_items = []
    
    for item in data_list:
        try:
            value = float(item.get(field, 0))
            identifier = item.get(identifier_field, '-')
            
            # Extract Asset Type from item data
            asset_type = extract_asset_type(item)
            
            if value == 0.0:
                failed_items.append({
                    'identifier': identifier,
                    'asset_type': asset_type,
                    'field': field,
                    'value': value,
                    'issue': 'zero_quantity',
                    'raw_data': item
                })
            else:
                passed_items.append({
                    'identifier': identifier,
                    'asset_type': asset_type,
                    'field': field,
                    'value': value,
                    'issue': 'zero_quantity',
                    'raw_data': item
                })
        except (ValueError, TypeError):
            # Handle invalid values - treat as zero
            asset_type = extract_asset_type(item)
            failed_items.append({
                'identifier': item.get(identifier_field, '-'),
                'asset_type': asset_type,
                'field': field,
                'value': item.get(field, 'Invalid'),
                'issue': 'zero_quantity',
                'error': 'Invalid numeric value',
                'raw_data': item
            })
    
    return failed_items, passed_items, len(data_list)


def unchanged_value_check(data_a, data_b, field, identifier_field='Inv Id'):
    """
    Check for unchanged values between two datasets
    
    Args:
        data_a: First dataset (list of items)
        data_b: Second dataset (list of items)
        field: Field name to compare
        identifier_field: Field to use as identifier for matching items
    
    Returns:
        tuple: (failed_items, passed_items, total_items)
    """
    failed_items = []
    passed_items = []
    
    # Create lookup dictionary for data_a
    data_a_lookup = {item.get(identifier_field): item for item in data_a}
    
    for item_b in data_b:
        # if field value is null, skip
        if item_b.get(field) is None:
            continue
        
        identifier = item_b.get(identifier_field, '-')
        item_a = data_a_lookup.get(identifier)

        # if item_a is not found, skip
        if item_a is None:
            continue
        
        # For portfolio validations, use Description from extra_data as display identifier
        display_identifier = identifier
        if 'extra_data' in item_b and item_b['extra_data']:
            try:
                import json
                extra_data = json.loads(item_b['extra_data'])
                if 'description' in extra_data:
                    display_identifier = extra_data['description']
            except (json.JSONDecodeError, TypeError):
                pass
        
        if item_a:
            try:
                # Check if field value in item_a is null/None - if so, don't flag as exception
                field_value_a = item_a.get(field)
                if field_value_a is None:
                    # Source A has null value - this is not an unchanged price exception
                    asset_type = extract_asset_type(item_b)
                    passed_items.append({
                        'identifier': display_identifier,
                        'inv_id': identifier,
                        'asset_type': asset_type,
                        'field': field,
                        'value_a': None,
                        'value_b': float(item_b.get(field, 0)),
                        'change': '-',  # Set change as '-' for null source A
                        'change_value': None,
                        'percentage_change': None,
                        'precision_type': 'PERCENTAGE',
                        'threshold': 0,
                        'is_failed': False,
                        'threshold_exceeded': False,
                        'display_change': '-',  # Display as '-'
                        'tooltip_change': '-',  # Tooltip also shows '-'
                        'issue': 'unchanged_value',
                        'note': 'Source A has null value - not an unchanged price exception',
                        'raw_data_a': item_a,
                        'raw_data_b': item_b
                    })
                    continue
                
                value_a = float(field_value_a)
                value_b = float(item_b.get(field, 0))
                
                # Extract Asset Type from item data
                asset_type = extract_asset_type(item_b)
                
                if value_a == value_b:
                    failed_items.append({
                        'identifier': display_identifier,
                        'inv_id': identifier,  # Keep original for tracking
                        'asset_type': asset_type,
                        'field': field,
                        'value_a': value_a,
                        'value_b': value_b,
                        'issue': 'unchanged_value',
                        'raw_data_a': item_a,
                        'raw_data_b': item_b
                    })
                else:
                    passed_items.append({
                        'identifier': display_identifier,
                        'inv_id': identifier,  # Keep original for tracking
                        'asset_type': asset_type,
                        'field': field,
                        'value_a': value_a,
                        'value_b': value_b,
                        'issue': 'unchanged_value',
                        'raw_data_a': item_a,
                        'raw_data_b': item_b
                    })
            except (ValueError, TypeError):
                # Handle invalid values
                asset_type = extract_asset_type(item_b)
                failed_items.append({
                    'identifier': display_identifier,
                    'inv_id': identifier,  # Keep original for tracking
                    'asset_type': asset_type,
                    'field': field,
                    'value_a': item_a.get(field, 'Invalid'),
                    'value_b': item_b.get(field, 'Invalid'),
                    'issue': 'unchanged_value',
                    'error': 'Invalid numeric value',
                    'raw_data_a': item_a,
                    'raw_data_b': item_b
                })
        else:
            # Item not found in data_a
            asset_type = extract_asset_type(item_b)
            passed_items.append({
                'identifier': display_identifier,
                'inv_id': identifier,  # Keep original for tracking
                'asset_type': asset_type,
                'field': field,
                'value_a': None,
                'value_b': item_b.get(field),
                'change': '-',  # Set change as '-' for missing source A
                'change_value': None,
                'percentage_change': None,
                'precision_type': 'PERCENTAGE',
                'threshold': 0,
                'is_failed': False,
                'threshold_exceeded': False,
                'display_change': '-',  # Display as '-'
                'tooltip_change': '-',  # Tooltip also shows '-'
                'issue': 'unchanged_value',
                'note': 'Item not found in dataset A',
                'raw_data_a': None,
                'raw_data_b': item_b
            })
    
    return failed_items, passed_items, len(data_b)


def price_change_percentage_check(data_a, data_b, field, threshold, identifier_field='Inv Id'):
    """
    Check for price changes exceeding a percentage threshold
    
    Args:
        data_a: First dataset (list of items)
        data_b: Second dataset (list of items)
        field: Field name to compare (e.g., 'End Local Market Price')
        threshold: Percentage threshold (e.g., 0.10 for 10%)
        identifier_field: Field to use as identifier for matching items
    
    Returns:
        tuple: (failed_items, passed_items, total_items)
    """
    failed_items = []
    passed_items = []
    
    # Create lookup dictionary for data_a
    data_a_lookup = {item.get(identifier_field): item for item in data_a}
    
    for item_b in data_b:
        identifier = item_b.get(identifier_field, '-')
        item_a = data_a_lookup.get(identifier)
        
        # For portfolio validations, use Description from extra_data as display identifier
        display_identifier = identifier
        if 'extra_data' in item_b and item_b['extra_data']:
            try:
                import json
                extra_data = json.loads(item_b['extra_data'])
                if 'description' in extra_data:
                    display_identifier = extra_data['description']
            except (json.JSONDecodeError, TypeError):
                pass
        
        if item_a:
            # Get raw values to check for NULL/missing data first
            raw_value_a = item_a.get(field)
            raw_value_b = item_b.get(field)
            
            # Skip items with NULL/missing prices - these should be handled by missing price validation
            if (raw_value_a is None or raw_value_a == '' or 
                str(raw_value_a).lower() in ['nan', 'null', 'none'] or
                (isinstance(raw_value_a, float) and raw_value_a != raw_value_a)):
                continue
                
            if (raw_value_b is None or raw_value_b == '' or 
                str(raw_value_b).lower() in ['nan', 'null', 'none'] or
                (isinstance(raw_value_b, float) and raw_value_b != raw_value_b)):
                continue
            
            try:
                value_a = float(raw_value_a)
                value_b = float(raw_value_b)
                
                # Calculate percentage change
                if value_a != 0:
                    percentage_change = abs((value_b - value_a) / abs(value_a))
                    # Calculate signed percentage change for display using formula (B-A)/|A|
                    signed_percentage_change = ((value_b - value_a) / abs(value_a)) * 100
                else:
                    # Handle zero base value - only if value_b is also not null
                    percentage_change = 999999.99 if value_b != 0 else 0  # Use large number instead of inf
                    signed_percentage_change = 100 if value_b > 0 else (-100 if value_b < 0 else 0)
                
                # Extract Asset Type from item data
                asset_type = extract_asset_type(item_b)
                
                if percentage_change > threshold:
                    failed_items.append({
                        'identifier': display_identifier,
                        'inv_id': identifier,  # Keep original for tracking
                        'asset_type': asset_type,
                        'field': field,
                        'value_a': value_a,
                        'value_b': value_b,
                        'percentage_change': signed_percentage_change,  # Signed percentage change for display
                        'threshold': threshold,
                        'issue': 'major_price_change',
                        'raw_data_a': item_a,
                        'raw_data_b': item_b
                    })
                else:
                    passed_items.append({
                        'identifier': display_identifier,
                        'inv_id': identifier,  # Keep original for tracking
                        'asset_type': asset_type,
                        'field': field,
                        'value_a': value_a,
                        'value_b': value_b,
                        'percentage_change': signed_percentage_change,  # Signed percentage change for display
                        'threshold': threshold,
                        'issue': 'major_price_change',
                        'raw_data_a': item_a,
                        'raw_data_b': item_b
                    })
            except (ValueError, TypeError):
                # Handle invalid values
                asset_type = extract_asset_type(item_b)
                failed_items.append({
                    'identifier': identifier,
                    'asset_type': asset_type,
                    'field': field,
                    'value_a': item_a.get(field, 'Invalid'),
                    'value_b': item_b.get(field, 'Invalid'),
                    'issue': 'major_price_change',
                    'error': 'Invalid numeric value',
                    'raw_data_a': item_a,
                    'raw_data_b': item_b
                })
        else:
            # Item not found in data_a
            asset_type = extract_asset_type(item_b)
            passed_items.append({
                'identifier': identifier,
                'asset_type': asset_type,
                'field': field,
                'value_a': None,
                'value_b': item_b.get(field),
                'issue': 'major_price_change',
                'note': 'Item not found in dataset A',
                'raw_data_a': None,
                'raw_data_b': item_b
            })
    
    return failed_items, passed_items, len(data_b)


def create_detailed_validation_result(validation_type, subtype, subtype2, failed_items, passed_items, threshold=None, kpi_info=None):
    """
    Create a detailed validation result dictionary
    
    Args:
        validation_type: Type of validation (e.g., 'PnL')
        subtype: Subtype of validation (e.g., 'Pricing')
        subtype2: Specific validation (e.g., 'Major Price Change')
        failed_items: List of items that failed validation
        passed_items: List of items that passed validation
        threshold: Threshold value used for validation
        kpi_info: Additional KPI information dictionary
    
    Returns:
        dict: Detailed validation result
    """
    kpi_info = kpi_info or {}
    
    return {
        'type': validation_type,
        'subType': subtype,
        'subType2': subtype2,
        'message': 1 if len(failed_items) > 0 else 0,
        'data': {
            'count': len(failed_items),
            'total_checked': len(failed_items) + len(passed_items),
            'passed_count': len(passed_items),
            'threshold': threshold,
            'precision_type': kpi_info.get('precision_type', 'PERCENTAGE'),  # Add precision_type to validation data
            'kpi_code': kpi_info.get('kpi_code', ''),
            'kpi_name': kpi_info.get('kpi_name', ''),
            'kpi_id': kpi_info.get('id', ''),
            'kpi_description': kpi_info.get('description', ''),
            'failed_items': failed_items,
            'passed_items': passed_items
        }
    }


def create_default_validation_result(validation_type, subtype, subtype2, failed_items, passed_items):
    """
    Create a default validation result for non-KPI validations
    
    Args:
        validation_type: Type of validation (e.g., 'PnL')
        subtype: Subtype of validation (e.g., 'Pricing')
        subtype2: Specific validation (e.g., 'Null/Missing Price')
        failed_items: List of items that failed validation
        passed_items: List of items that passed validation
    
    Returns:
        dict: Default validation result
    """
    return {
        'type': validation_type,
        'subType': subtype,
        'subType2': subtype2,
        'message': 1 if len(failed_items) > 0 else 0,
        'data': {
            'count': len(failed_items),
            'total_checked': len(failed_items) + len(passed_items),
            'passed_count': len(passed_items),
            'validation_source': 'default',
            'failed_items': failed_items,
            'passed_items': passed_items
        }
    }
