from datetime import datetime, timedelta


def decode_time(code):
    time_code = int(code[3:])
    minutes = (time_code - 1) * 30
    decoded_time = timedelta(minutes=minutes)
    hours, remainder = divmod(decoded_time.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_time = f"{hours:02}:{minutes:02}:{seconds:02}"

    return decoded_time


def decode_date(code):
    day_code = int(code[:3])
    start_date = datetime(2009, 1, 1)
    return start_date + timedelta(days=day_code - 1)


def decode_datetime_to_unix(code):
    decoded_date = decode_date(code)
    decoded_time = decode_time(code)

    combined_datetime = decoded_date + decoded_time
    unix_time = int(combined_datetime.timestamp())
    return unix_time
