import re
from datetime import datetime

import urllib


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


