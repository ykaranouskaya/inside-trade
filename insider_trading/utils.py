import re
from datetime import datetime


def parse_date(filename):
    """
    Find date from the string `filename`.
    """
    match = re.search(r'form.(\d+).idx', filename)
    if match:
        return match.group(1)


def get_quarter(date):
    """Compute quarter from the date"""
    quarter = {0: 'QTR4',
               1: 'QTR1',
               2: 'QTR2',
               3: 'QTR3'}
    quarter_ind = date.month // 4 + 1
    return quarter[quarter_ind]


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

