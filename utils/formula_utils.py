"""
Formula Utilities - Helper functions for formula processing
This module is separate to avoid circular import dependencies
"""

def extractFormulaFromDisplayName(formula_with_displayname: str):
    """
    Extract the display name (if present) and the actual formula.
    
    Args:
        formula_with_displayname: Formula string that may contain a display name prefix
        
    Returns:
        tuple: (displayname, formula) where displayname is None if not present
    """
    if not formula_with_displayname:
        return None, ""
    
    text = formula_with_displayname.strip()

    # Check if it starts with "Displayname:"
    if text.startswith("Displayname:"):
        # Split only on the first newline
        lines = text.split('\n', 1)

        displayname_line = lines[0].strip()
        displayname = displayname_line[len("Displayname:"):].strip()

        formula = lines[1].strip() if len(lines) > 1 else ""

        return displayname, formula
    
    # No Displayname prefix found
    return None, text

