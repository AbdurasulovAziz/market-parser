from datetime import datetime, timezone, timedelta


def generate_month_ranges(months=3):
    now = datetime.now(timezone.utc)

    current = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    for _ in range(months):
        first_day = current

        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)

        last_day = next_month - timedelta(seconds=1)

        yield (
            first_day.strftime("%Y-%m-%dT%H:%M:%SZ"),
            last_day.strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        current = (current - timedelta(days=1)).replace(day=1)