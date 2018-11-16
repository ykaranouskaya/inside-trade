from datetime import datetime, timedelta
import logging
import time

from insider_trading.data_parser import Index, utils

INVALID_NAMES = [' llc', ' lp', 'group', 'trust', 'associates', 'l.p.', 'holdings', 'inc.', 'partners']

logger = logging.getLogger(__name__)


def _valid_name(name):
    for item in INVALID_NAMES:
        if item in name.lower():
            return False
    return True


def _valid_position(position):
    if position == '1' or position.lower() == 'true':
        return True
    return False


def _filter_valid_form(form):
    form_content = form.get_content()

    # 1. Check owner name
    owner_name = form_content['owner']['name']

    # 2. Check position
    isdirector = form_content['owner']['isdirector']
    isofficer = form_content['owner']['isofficer']

    if _valid_name(owner_name) and (_valid_position(isdirector) or _valid_position(isofficer)):
        return True
    else:
        return False


def _make_row(entry, transaction):
    # heading = (['OWNER_CIK', 'OWNER_NAME', 'IS_DIRECTOR', 'IS_OFFICER', 'IS_10%_OWNER', 'OTHER', 'COMMENTS',
    #             'ISSUER_CIK', 'ISSUER_COMPANY', 'TICKER',
    #             'EQUITY', 'TRANSACTION_DATE', 'AQUIRED/DISPOSED', 'AMOUNT', 'PRICE_PER_UNIT',
    #             'HOLDING_BEFORE', 'HOLDING_AFTER', 'OWNERSHIP_STATUS(DIRECT/INDIRECT)',
    #             'OWNERSHIP_NATURE'])
    row = []
    for value in entry['owner'].values():
        row.append(value)
    for value in entry['issuer'].values():
        row.append(value)
    for value in list(entry['transactions'][transaction].values())[:-1]:
        row.append(value)
    row.append(entry['holding']['holding_before'])
    row.append(entry['transactions'][transaction]['holding_after'])
    row.append(entry['holding']['ownership_status'])
    row.append(entry['holding']['ownership_nature'])

    return row


def get_daily_data(date):
    daily_data = []

    index_name = utils.create_index_filename(date)
    index_url = utils.index_url_from_date(date)
    index = Index(index_url, index_name)

    for f in index.generate_form():
        try:
            f.extract_info()
            time.sleep(0.5)
        except AttributeError as e:
            logger.warning(f"Error parsing {str(f)}")
            continue
        content = f.get_content()
        # print(f"Got content for form {form} !")

        if _filter_valid_form(f):
            daily_data.append(content)

    return daily_data


def generate_csv_row(daily_data):

    for entry in daily_data:
        for transaction in entry['transactions']:
            row = _make_row(entry, transaction)
            yield row


if __name__ == '__main__':
    index = Index('2018/QTR3/form.20180916.id')
    form_gen = index.generate_form()
    form = next(form_gen)
    form.extract_info()
    daily_data = [form.get_content()]
    daily_data = [{'owner': {'cik': '0001260937',
                             'name': 'ABBRECHT TODD M',
                             'isdirector': 'true',
                             'isofficer': '',
                             'istenpercentowner': '',
                             'isother': '',
                             'officertitle': ''},
                   'issuer': {'cik': '0001610950',
                              'company': 'Syneos Health, Inc.',
                              'ticker': 'SYNH'},
                   'transactions': {'transaction1': {'security': 'Class A Common Stock',
                                                     'date': '2018-09-13',
                                                     'code': 'D',
                                                     'amount': '2178',
                                                     'price': '0',
                                                     'holding_after': '4103'}},
                   'holding': {'holding_before': '4103',
                               'ownership_status': 'D',
                               'ownership_nature': ''}}]
    print(daily_data[0])
    row_gen = generate_csv_row(daily_data)
    print(next(row_gen))
