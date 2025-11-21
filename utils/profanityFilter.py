"""
Utility module for profanity filtering across the application
"""
import os
import re
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Check if profanity filter is available
ENABLE_PROFANITY_FILTER = os.getenv("ENABLE_PROFANITY_FILTER", "true").lower() == "true"

# Try to use better-profanity (simpler, no complex dependencies)
profanity = None
profanity_filter = None
PROFANITY_FILTER_AVAILABLE = False

try:
    from better_profanity import profanity
    PROFANITY_FILTER_AVAILABLE = True
    # Load the censor words on import
    profanity.load_censor_words()
    logger.info("Profanity filtering enabled using better-profanity")
except (ImportError, Exception) as e:
    logger.debug(f"better-profanity not available: {e}")
    # Fallback to profanity-filter if better-profanity is not available
    try:
        from profanity_filter.profanity_filter import ProfanityFilter
        PROFANITY_FILTER_AVAILABLE = True
        profanity_filter = ProfanityFilter()
        logger.info("Profanity filtering enabled using profanity-filter")
    except (ImportError, Exception) as e2:
        try:
            # Try alternative import path
            from profanity_filter import ProfanityFilter
            PROFANITY_FILTER_AVAILABLE = True
            profanity_filter = ProfanityFilter()
            logger.info("Profanity filtering enabled using profanity-filter (alternative import)")
        except (ImportError, Exception) as e3:
            PROFANITY_FILTER_AVAILABLE = False
            logger.warning(f"Profanity filtering not available. better-profanity error: {e}, profanity-filter errors: {e2}, {e3}")

# Common profanity words that need to be caught even within other words
# This list includes common profanities that might appear in document data
PROFANITY_PATTERNS = [
    r'\basshole\b',
    r'asshole',
    r'\bdamn\b',
    r'\bhell\b',
    r'\bshit\b',
    r'\bfuck\b',
    r'\bbitch\b',
    r'\bcunt\b',
    r'\bpiss\b',
    r'\bcock\b',
    r'\bdick\b',
    r'\btits\b',
    r'\btwat\b',
    r'\bwhore\b',
    r'\bslut\b',
    r'\bfag\b',
    r'\bnigger\b',
    r'\bspic\b',
    r'\bkike\b',
    r'\bchink\b',
]

# Leetspeak character mappings for common profanity evasion
LEETSPEAK_MAP = {
    'a': '[aA@4]',
    'e': '[eE3]',
    'i': '[iI1!|]',
    'o': '[oO0]',
    's': '[sS$5]',
    't': '[tT7]',
    'l': '[lL1|I]',
    'z': '[zZ2]',
    'g': '[gG6]',
    'b': '[bB8]',
}

def _create_leetspeak_pattern(word: str) -> str:
    """Create a regex pattern that matches leetspeak variations of a word"""
    pattern = ''
    for char in word.lower():
        if char in LEETSPEAK_MAP:
            pattern += LEETSPEAK_MAP[char]
        else:
            pattern += f'[{char}{char.upper()}]'
    return pattern

def _censor_profane_words(text: str) -> str:
    """Censor profane words in text by replacing with asterisks"""
    if not text or not isinstance(text, str):
        return text
    
    if not PROFANITY_FILTER_AVAILABLE:
        return text
    
    try:
        # First, use better-profanity for standard whole-word matching
        if profanity is not None:
            censored_text = profanity.censor(text)
        # Fallback to profanity-filter
        elif profanity_filter is not None:
            censored_text = profanity_filter.censor(text)
        else:
            censored_text = text
        
        # Additional aggressive filtering for profanity within words and leetspeak
        # This catches cases like "AssholeSystem" or "AsshoIeSystem"
        # We need to work backwards through matches to preserve indices
        text_lower = censored_text.lower()
        replacements = []
        
        # Check for profanity patterns (including within words)
        for pattern_str in PROFANITY_PATTERNS:
            if pattern_str.startswith(r'\b'):
                # Whole word pattern - skip, better-profanity handles this
                continue
            pattern = re.compile(pattern_str, re.IGNORECASE)
            matches = list(pattern.finditer(text_lower))
            for match in matches:
                start, end = match.span()
                profane_word = censored_text[start:end]
                asterisks = '*' * len(profane_word)
                replacements.append((start, end, asterisks))
        
        # Also check for leetspeak variations
        for profanity_word in ['asshole', 'damn', 'hell', 'shit', 'fuck', 'bitch']:
            leet_pattern = _create_leetspeak_pattern(profanity_word)
            pattern = re.compile(leet_pattern, re.IGNORECASE)
            matches = list(pattern.finditer(censored_text))
            for match in matches:
                start, end = match.span()
                profane_word = censored_text[start:end]
                asterisks = '*' * len(profane_word)
                # Check if this region is already censored
                if profane_word != asterisks and '*' not in profane_word:
                    replacements.append((start, end, asterisks))
        
        # Sort replacements by start position (descending) to replace from end to start
        replacements.sort(key=lambda x: x[0], reverse=True)
        
        # Apply replacements
        for start, end, asterisks in replacements:
            censored_text = censored_text[:start] + asterisks + censored_text[end:]
        
        return censored_text
    except Exception as e:
        logger.debug(f"Error censoring profanity: {e}")
        # Return original text if filtering fails
        return text

def filter_profanity_in_data(data: Any) -> Any:
    """Recursively filter profanity from response data"""
    if not ENABLE_PROFANITY_FILTER or not PROFANITY_FILTER_AVAILABLE:
        return data
    
    if isinstance(data, dict):
        return {key: filter_profanity_in_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [filter_profanity_in_data(item) for item in data]
    elif isinstance(data, str):
        # Censor profane words in the string using profanity-filter library
        return _censor_profane_words(data)
    else:
        return data

