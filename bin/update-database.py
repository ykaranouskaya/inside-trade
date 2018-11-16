# !usr/bin/python

import argparse
import csv
import sys
from datetime import datetime
import time
from pytz import timezone
from pathlib import Path
import logging

from insider_trading import download_index, parse_index, store, utils
from insider_trading.utils import check_same_date


BASE_ENDPOINT = 'https://www.sec.gov/Archives/'
TIMEZONE = timezone('EST')


logger = logging.getLogger(__name__)


def parse_arguments(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument('output_database', help='Data file to save parsed info')
    parser.add_argument('--date', help='Get data for this date formatted as "%Y-%m-%d"',
                        default=datetime.now(tz=TIMEZONE))
    parser.add_argument('--update-range', dest='update', action='store_true',
                        help='Download indices from the latest date in `output-folder`')
    parser.add_argument('--end-date', dest='end_date', help='Range end date formatted as "%Y-%m-%d"',
                        default=datetime.now(tz=TIMEZONE))

    return parser.parse_args(argv)


def create_data_csv(filepath):
    heading = (['REPORT_DATE', 'OWNER_CIK', 'OWNER_NAME', 'IS_DIRECTOR', 'IS_OFFICER', 'IS_10%_OWNER', 'OTHER',
                'COMMENTS', 'ISSUER_CIK', 'ISSUER_COMPANY', 'TICKER',
                'EQUITY', 'TRANSACTION_DATE', 'AQUIRED/DISPOSED', 'AMOUNT', 'PRICE_PER_UNIT',
                'HOLDING_BEFORE', 'HOLDING_AFTER', 'OWNERSHIP_STATUS(DIRECT/INDIRECT)',
                'OWNERSHIP_NATURE'])

    with open(filepath, 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(heading)


def append_daily_info_to_database(date, database):
    """
    Get daily info and write to database.
    :param date: str, format "%Y-%m-%d"
    :param database: csv file
    """
    date = utils.to_date(date)
    print(f'Updating database for {date.strftime("%Y-%m-%d")}')
    daily_data = store.get_daily_data(date)
    logger.info(f'Updating database for {date.strftime("%Y-%m-%d")}')
    with open(database, 'a') as db:
        csv_writer = csv.writer(db)
        for row in store.generate_csv_row(daily_data):
            row.insert(0, date.strftime("%Y-%m-%d"))
            csv_writer.writerow(row)
            print(f"\tNew row added to {database}")


def append_date_range(start_date, end_date, database):
    start_date = utils.to_date(start_date)
    end_date = utils.to_date(end_date)
    days = utils.find_weekdays(start_date, end_date)

    for day in days:
        append_daily_info_to_database(day, database)


def main(argv=None):
    logger.setLevel('DEBUG')

    # Download index first
    argv = argv or sys.argv[1:]
    args = parse_arguments(argv)

    data_db = Path(args.output_database)
    # date = datetime.now(tz=TIMEZONE)
    date = args.date

    # Check if database exists
    if not data_db.exists():
        create_data_csv(data_db)

    # Update if necessary
    # if args.update:
    #     latest = download_index.find_latest_downloaded_index(data_db)
    #     print(f"    FROM: {latest}")
    #     print(f"    UNTIL: {date}")
    #
    #     if check_same_date(date, latest):
    #         print("Already up to date.")
    #         sys.exit(0)
    #
    #     inds = download_index.download_span_indices((latest, date), data_db)

    if not args.update:
        # TODO: Check if this date is already in the database
        append_daily_info_to_database(date, data_db)

    else:
        end_date = args.end_date
        append_date_range(date, end_date, data_db)




    # Parse index
    # if not data_csv.exists():
    #     create_data_csv(data_csv)
    #
    # for index in inds:
    #     form = Path(output_folder) / index
    #     print(f"Parsing {index}...")
    #
    #     for entry in parse_index.read_form_index_entries(form):
    #         print(entry)
    #
    #         form_url = BASE_ENDPOINT + entry[-1]
    #         info = parse_index.parse_form(form_url)
    #         time.sleep(1)
    #         print(f"OWNER: {info[0]}")
    #         print(f"ISSUER: {info[1]}")
    #         print(f"TRANSACTIONS: {info[2]}")
    #         print(f"HOLDINGS: {info[3]}")
    #
    #         with open(data_csv, 'a') as fout:
    #             writer = csv.writer(fout)
    #             for entry in parse_index.generate_csv_entry(info):
    #                 print(','.join(entry))
    #                 writer.writerow(entry)


if __name__ == "__main__":
    main()
