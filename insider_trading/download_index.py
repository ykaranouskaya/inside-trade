# !usr/bin/python

from datetime import datetime, timedelta
import time
import urllib.request as request
import urllib.parse
from pathlib import Path

from insider_trading import utils


DAILY_INDEX_ENDPOINT = 'https://www.sec.gov/Archives/edgar/daily-index/'


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
    quarter = utils.get_quarter(date)
    filename = utils.create_index_filename(date)
    url_stem = '/'.join([year, quarter, filename])
    url_address = urllib.parse.urljoin(DAILY_INDEX_ENDPOINT, url_stem)
    print(url_address)

    try:
        req = request.Request(url_address, headers={'User-Agent': 'Mozilla/5.0'})
        with request.urlopen(req) as url_in:
            data = url_in.read()
    except urllib.error.HTTPError as e:
        print('Error while downloading index')
        print(e)
        return None

    try:
        output = Path(output_folder)
        with open(output / filename, 'wb') as f_out:
            f_out.write(data)
    except FileNotFoundError:
        print("No such output folder!")

    return filename


def download_span_indices(date_span, output_folder):
    """
    Donwload indicies for a time span (start_date, end_date] inclusive to `output_folder`.
    :param date_span: tuple of datetime.dates (start_date, end_date)
    :param output_folder: where to write indicies
    :return: list of indices filenames
    """
    # Find valid weekdays (omit weekends)
    days = find_weekdays(date_span[0], date_span[1])
    inds = []
    for day in days:
        index = download_day_form_index(day, output_folder)
        if index:
            inds.append(index)
        time.sleep(1)

    return inds


def find_latest_downloaded_index(data_folder):
    """
    Find latest downloaded index in `data_folder`
    :param data_folder:
    :return: datetime date
    """
    p = Path(data_folder)
    latest = None
    for index in p.glob('./*.idx'):
        date_str = utils.parse_date(index.name)
        cur_date = datetime.strptime(date_str, '%Y%m%d')
        # print(f"DATE: {cur_date}")
        if latest:
            if (cur_date - latest).days > 0:
                latest = cur_date
                # print(f"DELTA: {(cur_date - latest).days}")
        else:
            latest = cur_date
    return latest


if __name__ == "__main__":
    date = datetime.now()
    print(f"DATE: {date - timedelta(1)}")
    latest = find_latest_downloaded_index('data')
    print(f"LAST DATE: {latest}")

    # url = download_day_form_index(date - timedelta(1), 'data')
    # print(f"URL: {url}")
    # download_span_indices((date - timedelta(10), date), 'data')
