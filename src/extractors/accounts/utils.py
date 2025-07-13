from datetime import datetime, timedelta

def parse_date(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f0Z")
    return dt, "0"

def build_date_str(dt, extra_digit):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + extra_digit + "Z"

def generate_year_intervals(start_date_str):
    parsed_start_date, _ = parse_date(start_date_str)
    today = datetime.utcnow()

    intervals = []
    year_start = parsed_start_date

    while year_start < today:
        year_end = datetime(year_start.year + 1, 1, 1)
        if year_end > today:
            year_end = today
        intervals.append((year_start, year_end))
        year_start = year_end

    return intervals

