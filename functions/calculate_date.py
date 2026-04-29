# functions/calculate_date.py
from datetime import datetime, timedelta
from typing import Dict, Any
from services.call_context import CallContext
from services.text_utils import extract_words

async def calculate_date(context: CallContext, args: Dict[str, Any]) -> str:
    """
    Utility function to calculate a specific date based on user input like 'next Monday' or 'coming Monday'.
    Args:
        context: CallContext object
        args: Dictionary containing 'phrase' (e.g., 'next Monday', 'coming Monday') and optional 'future_intent' (boolean)
    Returns:
        A string representing the calculated date in 'MM, DD, YYYY' format.
    """
    phrase = args.get('phrase', '').lower()
    future_intent = args.get('future_intent', True)  # Default to assuming future events (e.g., scheduling)
    
    today = datetime.now()
    weekday_map = {
        'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
        'friday': 4, 'saturday': 5, 'sunday': 6
    }

    # Extract day from phrase
    target_day = None
    for token in extract_words(phrase, allow_apostrophe=False):
        if token in weekday_map:
            target_day = token
            break
    if not target_day:
        return "Invalid day specified."
    target_weekday = weekday_map[target_day]
    current_weekday = today.weekday()

    if 'next' in phrase:
        # Next week: Move to the next week's instance of the target day
        days_until_target = (target_weekday - current_weekday + 7) % 7
        if days_until_target == 0:
            days_until_target = 7  # Ensure we move to the next week
        days_until_target += 7  # Add another week to ensure next week
    elif 'coming' in phrase or 'this' in phrase:
        # Current week: Find the target day in the current week
        days_until_target = (target_weekday - current_weekday + 7) % 7
        # If the day has passed and future intent is implied, move to next week
        if days_until_target != 0 and days_until_target < 7 and future_intent:
            days_until_target += 7
    else:
        # Default: Assume nearest upcoming day
        days_until_target = (target_weekday - current_weekday + 7) % 7
        if days_until_target == 0 and future_intent:
            days_until_target = 7  # If today is the target day, assume next week for future intent

    target_date = today + timedelta(days=days_until_target)
    return target_date.strftime("%m, %d, %Y")
