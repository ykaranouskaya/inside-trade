# !usr/bin/python

from datetime import datetime, timedelta
import time
import urllib.request as request
import urllib.parse
from pathlib import Path


DAILY_INDEX_ENDPOINT = 'https://www.sec.gov/Archives/edgar/daily-index/'


def get_quarter(date):
    """Compute quarter from the date"""
    quarter = {0: 'QTR4',
               1: 'QTR1',
               2: 'QTR2',
               3: 'QTR3'}
    quarter_ind = date.month // 4 + 1
    return quarter[quarter_ind]


def find_weekdays(start_date, end_date):
    """Find weekdays from the span (start_date, end_date) `start_date` exclusive."""
    # start_date = start_date.date()
    # end_date = start_date.date()

    date = start_date
    weekdays = []
    while date != end_date:
        date += timedelta(1)
        # print(f"New date: {date.weekday()}")
        if date.isoweekday() in range(1, 6):
            weekdays.append(date)

    return weekdays


def download_day_form_index(date, output_folder):
    """
    Downloads forms for a specific day and save to `output_folder`
    :param date: datetime.date instance
    """
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')
    quarter = get_quarter(date)
    stamp = ''.join([year, month, day])
    filename = f'form.{stamp}.idx'
    url_stem = '/'.join([year, quarter, filename])
    url_address = urllib.parse.urljoin(DAILY_INDEX_ENDPOINT, url_stem)
    print(url_address)

    try:
        req = request.Request(url_address, headers={'User-Agent': 'Mozilla/5.0'})
        with request.urlopen(req) as url_in:
            data = url_in.read()
    except urllib.error.HTTPError as e:
        raise e

    try:
        output = Path(output_folder)
        with open(output / filename, 'wb') as f_out:
            f_out.write(data)
    except FileNotFoundError:
        print("No such output folder!")

    return url_address


def download_span_indices(date_span, output_folder):
    """
    Donwload indicies for a time span (start_date, end_date] inclusive to `output_folder`.
    :param date_span: tuple of datetime.dates (start_date, end_date)
    :param output_folder:
    """
    # Find valid weekdays (omit weekends)
    days = find_weekdays(date_span[0], date_span[1])
    for day in days:
        download_day_form_index(day, output_folder)


if __name__ == "__main__":
    date = datetime.now()
    print(f"DATE: {date - timedelta(1)}")
    # url = download_day_form_index(date - timedelta(1), 'data')
    # print(f"URL: {url}")
    # download_span_indices((date - timedelta(10), date), 'data')
