import re
from datetime import datetime, timedelta

import urllib


def get_current_date(str_format=True):
    date = datetime.now()
    if str_format:
        date = date.strftime("%Y-%m-%d")

    return date


def strip_date(date):
    pattern = '\d{4}-\d{1,2}-\d{1,2}'
    match = re.search(pattern, date)
    if match:
        return match.group()
    else:
        raise ValueError(f'Incorrect date format: {date}')


def to_date(date):
    if isinstance(date, str):
        # extract only date
        date = strip_date(date)
        date = datetime.strptime(date, "%Y-%m-%d")
    return date


def is_out_of_date(date, last_refreshed, last_date, date_window):
    date = to_date(date)
    last_refreshed = to_date(last_refreshed)
    last_date = to_date(last_date)

    old = check_days_diff(date, last_date, diff=date_window)
    if old and last_refreshed <= last_date:
        return True

    return False


def find_weekdays(start_date, end_date):
    """Find weekdays from the span (start_date, end_date) `start_date` exclusive."""
    # start_date = start_date.date()
    # end_date = start_date.date()
    date = start_date
    weekdays = []
    while not check_same_date(date, end_date):
        date += timedelta(days=1)
        # print(f"New date: {date.weekday()}")
        if date.isoweekday() in range(1, 6):
            weekdays.append(date)

    return weekdays


def append_valid_string(tag_name):
    if tag_name:
        return tag_name.text.strip()
    else:
        return ''


def index_url_from_date(date):
    year = date.strftime('%Y')
    quarter = get_quarter(date)
    filename = create_index_filename(date)
    url_stem = '/'.join([year, quarter, filename])
    return url_stem


def parse_date(filename):
    """
    Find date from the string `filename`.
    """
    match = re.search(r'form.(\d+).idx', filename)
    if match:
        return match.group(1)


def get_quarter(date):
    """Compute quarter from the date"""
    quarter = {1: 'QTR1',
               2: 'QTR2',
               3: 'QTR3',
               4: 'QTR4'}
    quarter_ind = (date.month - 1) // 3 + 1
    return quarter[quarter_ind]


def check_same_date(date1, date2):
    """Check if two datetime dates are the same days"""
    if date1.year == date2.year and date1.month == date2.month and date1.day == date2.day:
        return True
    else:
        return False


def create_index_filename(date):
    """
    Compose forms index filename from the date
    :param date: datetime object
    :return: string
    """
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')
    stamp = ''.join([year, month, day])
    filename = f'form.{stamp}.idx'
    return filename


def check_days_diff(date1, date2, diff=180):
    """ Check if timedelta < then `diff` for two dates.
    """
    assert isinstance(date1, datetime)
    assert isinstance(date2, datetime)

    delta = (date2 - date1).days
    return delta < diff


def is_valid_ticker(ticker):
    # Check if all chars are  alphabetic
    if ticker.isalpha():
        return True
    return False
