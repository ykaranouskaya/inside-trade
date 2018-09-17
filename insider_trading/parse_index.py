# !usr/bin/python

import datetime
import time
import re
import csv
import xml.etree.cElementTree as cElementTree
import urllib.request as request
from pathlib import Path
import numpy as np
from bs4 import BeautifulSoup

BASE_ENDPOINT = 'https://www.sec.gov/Archives/'
DAILY_INDEX_ENDPOINT = 'https://www.sec.gov/Archives/edgar/daily-index/'


def generate_entry(url):
    """
     Generate location of a form from the index xml.
     """
    with request.urlopen(url) as urlfile:
        for event, elem in cElementTree.iterparse(urlfile):
            if elem.tag[-3:] == 'loc':
                yield elem.text


def parse_entry(entry):
    """
     Parse index form entry:
          Form Company CIK Date Filename
     """
    entry_list = entry.strip().split(' ')
    entry_list = list(filter(lambda x: x != '', entry_list))
    form = entry_list[0]
    company = ' '.join(entry_list[1:-3])
    cik = entry_list[-3]
    date = entry_list[-2]
    filename = entry_list[-1]

    return (form, company, cik, date, filename)


def read_form_index_entries(filename):
    """
     Read form index from file and return 4/A form entries iteratively.
     """
    with open(filename, 'r') as f:
        for line in f:
            if line.startswith('4/A'):
                entry = parse_entry(line)
                yield entry


def append_valid_string(tag_name):
    if tag_name:
        return tag_name.text.strip()
    else:
        return ''


def _extract_owner_info(soup):
    """
     Find information related to reporting owner from bs4 soup.
     :param soup: bs4 soup object
     :return: tuple (cik, name, ownership_relation:[Director, Officer, 10% Owner, Other])
     """
    reporting_owner = soup.reportingowner
    id = reporting_owner.reportingownerid
    relation = reporting_owner.reportingownerrelationship
    cik = id.rptownercik.text
    name = id.rptownername.text
    ownership_relation = []

    # Append ownership relation
    ownership_relation.append(append_valid_string(relation.isdirector))
    ownership_relation.append(append_valid_string(relation.isofficer))
    ownership_relation.append(append_valid_string(relation.istenpercentowner))
    ownership_relation.append(append_valid_string(relation.isother))
    ownership_relation.append(append_valid_string(relation.officertitle))

    print(ownership_relation)
        # print(child.tag)
        # ownership_relation[child.tag] = child.text.strip()
    # if len(ownership_relation) == 4:
    #     ownership_relation.append('')

    return (cik, name, ownership_relation)


def _extract_transaction_info(soup):
    """
     Find information from nonDerivativeTransaction.
     :param soup: bs4 soup object
     :return: tuple (security, date, code(A/D), amount, price, holding_after)
     """
    transactions = soup.nonderivativetable.find_all('nonderivativetransaction')
    # print(f"Transactions: {transactions}")
    trans_tuple = []
    for transaction in transactions:
        security = transaction.securitytitle.text.strip()
        date = transaction.transactiondate.text.strip()
        trans_amounts = transaction.transactionamounts
        code = trans_amounts.transactionacquireddisposedcode.text.strip()
        amount = trans_amounts.transactionshares.text.strip()
        price = trans_amounts.transactionpricepershare.text.strip()
        holding_after = transaction.posttransactionamounts.sharesownedfollowingtransaction.text.strip()

        trans_tuple.append((security, date, code, amount, price, holding_after))

    return trans_tuple


def _extract_issuer_info(soup):
    """
     Find information about issuer.
     :param soup: bs4 soup object
     :return: tuple (cik, company, ticker)
     """
    issuer = soup.issuer
    cik = issuer.issuercik.text
    company = issuer.issuername.text
    ticker = issuer.issuertradingsymbol.text

    return (cik, company, ticker)


def _extract_holding_info(soup):
    """
     Find before/after holdings info.
     :param soup: bs4 soup object
     :return: tuple (hoding_before, ownership_status(Direct/Indirect), ownership_nature)
     """
    holding = soup.nonderivativetable

    try:
        holding_before = holding.posttransactionamounts.sharesownedfollowingtransaction.text.strip()
    except AttributeError:
        holding_before = ''

    ownership = holding.ownershipnature
    try:
        ownership_status = ownership.directorindirectownership.text.strip()
    except AttributeError:
        ownership_status = ''
    try:
        ownership_nature = ownership.natureofownership.text.strip()
    except AttributeError:
        ownership_nature = ''

    return (holding_before, ownership_status, ownership_nature)


def parse_form(txturl, from_file=False):
    if from_file:
        with open('data/test_form1.txt', 'rb') as f:
            soup = BeautifulSoup(f, 'html.parser')
        # print(soup.prettify())
        # print(soup.reportingowner.reportingownerrelationship.string)
        # print(_extract_owner_info(soup))
        # print(_extract_transaction_info(soup))
        # print(_extract_issuer_info(soup))
        # print(_extract_holding_info(soup))
    else:
        with request.urlopen(txturl) as url:
            url_data = url.read()
        soup = BeautifulSoup(url_data, 'html.parser')
    owner_info = _extract_owner_info(soup)
    transactions = _extract_transaction_info(soup)
    issuer_info = _extract_issuer_info(soup)
    holdings_info = _extract_holding_info(soup)

    return (owner_info, issuer_info, transactions, holdings_info)


def generate_csv_entry(info):
    """
     Generate entry as a table: (OWNER_CIK, OWNER_NAME, IS_DIRECTOR, IS_OFFICER, IS_10%_OWNER, OTHER, COMMENTS,
                              ISSUER_CIK, ISSUER_COMPANY, TICKER,
                              EQUITY, TRANSACTION_DATE, AQUIRED/DISPOSED, AMOUNT, PRICE_PER_UNIT,
                              HOLDING_BEFORE, HOLDING_AFTER, OWNERSHIP_STATUS(DIRECT/INDIRECT), OWNERSHIP_NATURE)
     :param info: tuple
     :return:
     """
    owner = info[0]
    issuer = info[1]
    holding = info[3]
    for transaction in info[2]:
        entry = [owner[0], owner[1], *owner[2], *issuer, *transaction[:-1], holding[0],
                 transaction[-1], *holding[1:]]
        yield entry


if __name__ == "__main__":
    # url = DAILY_INDEX_ENDPOINT + '2018/QTR3/sitemap.20180824.xml'
    form = Path('./data') / 'form.20180914.idx'
    data_csv = Path('./data') / 'data.csv'
    heading = (['OWNER_CIK', 'OWNER_NAME', 'IS_DIRECTOR', 'IS_OFFICER', 'IS_10%_OWNER', 'OTHER', 'COMMENTS',
                'ISSUER_CIK', 'ISSUER_COMPANY', 'TICKER',
                'EQUITY', 'TRANSACTION_DATE', 'AQUIRED/DISPOSED', 'AMOUNT', 'PRICE_PER_UNIT',
                'HOLDING_BEFORE', 'HOLDING_AFTER', 'OWNERSHIP_STATUS(DIRECT/INDIRECT)',
                'OWNERSHIP_NATURE'])

    with open(data_csv, 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(heading)

    for entry in read_form_index_entries(form):
        print(entry)

        test_form_url = BASE_ENDPOINT + entry[-1]
        info = parse_form(test_form_url)
        time.sleep(1)
        print(f"OWNER: {info[0]}")
        print(f"ISSUER: {info[1]}")
        print(f"TRANSACTIONS: {info[2]}")
        print(f"HOLDINGS: {info[3]}")

        with open(data_csv, 'a') as fout:
            writer = csv.writer(fout)
            for entry in generate_csv_entry(info):
                print(','.join(entry))
                writer.writerow(entry)
