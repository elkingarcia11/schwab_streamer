"""
Utility functions for file operations
    - get_comma_separated_items_from_file
    - get_symbols_from_file
    - get_timeframes_from_file
"""

from typing import List


def get_comma_separated_items_from_file(filepath: str) -> List[str]:
    """
    Get comma-separated items from file (e.g., symbols or timeframes)
    
    Args:
        filepath: Path to the file containing comma-separated items
        
    Returns:
        List of items (stripped)
        
    Example:
        If file contains: "SPY,QQQ,META,AMZN"
        Returns: ["SPY", "QQQ", "META", "AMZN"]
    """
    try:
        with open(filepath, 'r') as file:
            content = file.read().strip()
            
            # Split by comma and clean up each item
            items = [item.strip() for item in content.split(',') if item.strip()]
            print( f"📄 Read {len(items)} items from {filepath}: {items}")
            return items
    except FileNotFoundError:
        print(f"⚠️ File not found: {filepath}")
        return []
    except Exception as e:
        print(f"❌ Error reading file {filepath}: {e}")
        return []


def get_symbols_from_file(symbols_filepath: str) -> List[str]:
    """Get symbols from file (alias for get_comma_separated_items_from_file)"""
    return get_comma_separated_items_from_file(symbols_filepath)


def get_timeframes_from_file(timeframes_filepath: str) -> List[str]:
    """Get timeframes from file (alias for get_comma_separated_items_from_file)"""
    return get_comma_separated_items_from_file(timeframes_filepath) 