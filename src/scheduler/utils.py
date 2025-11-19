from datetime import datetime, timedelta
from typing import List


def _parse_cron_field(field: str, min_val: int, max_val: int) -> List[int]:
    """Parse a single cron field and return list of matching values."""
    if field == "*":
        return list(range(min_val, max_val + 1))

    values = []
    for part in field.split(","):
        if "/" in part:
            range_part, step = part.split("/")
            step = int(step)
            if range_part == "*":
                values.extend(range(min_val, max_val + 1, step))
            else:
                start, end = map(int, range_part.split("-"))
                values.extend(range(start, end + 1, step))
        elif "-" in part:
            start, end = map(int, part.split("-"))
            values.extend(range(start, end + 1))
        else:
            values.append(int(part))

    return sorted(set(values))


def _get_next_cron_time(cron_expr: str, current_time: datetime) -> datetime:
    """Calculate next run time for a cron expression (minute hour day month weekday)."""
    parts = cron_expr.split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {cron_expr}")

    minutes = _parse_cron_field(parts[0], 0, 59)
    hours = _parse_cron_field(parts[1], 0, 23)
    days = _parse_cron_field(parts[2], 1, 31)
    months = _parse_cron_field(parts[3], 1, 12)
    weekdays = _parse_cron_field(parts[4], 0, 6)  # 0 = Sunday

    # Start from next minute
    next_time = current_time.replace(second=0, microsecond=0) + timedelta(minutes=1)

    # Search for next valid time (max 366 days to avoid infinite loop)
    for _ in range(366 * 24 * 60):
        if (
            next_time.minute in minutes
            and next_time.hour in hours
            and next_time.day in days
            and next_time.month in months
            and next_time.weekday() in [(d + 1) % 7 for d in weekdays]
        ):  # Convert Sun=0 to Mon=0
            return next_time
        next_time += timedelta(minutes=1)

    raise ValueError(f"Could not find next run time for cron: {cron_expr}")
