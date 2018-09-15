# !usr/bin/python

from datetime import datetime, timedelta
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
    url = urllib.parse.urljoin(DAILY_INDEX_ENDPOINT, url_stem)
    print(url)

    try:
        req = request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
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

    return url


if __name__ == "__main__":
    date = datetime.now()
    print(f"DATE: {date - timedelta(1)}")
    url = download_day_form_index(date - timedelta(1), 'data')
    print(f"URL: {url}")