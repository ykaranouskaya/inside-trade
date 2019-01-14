# !usr/bin/python

from datetime import datetime, timedelta
import time
import asyncio, aiohttp
import urllib.request as request
import urllib.parse
from pathlib import Path
import logging

from tqdm import tqdm
from bs4 import BeautifulSoup

from insider_trading import utils


BASE_FORM_ENDPOINT = 'https://www.sec.gov/Archives/'
DAILY_INDEX_ENDPOINT = 'https://www.sec.gov/Archives/edgar/daily-index/'
RATE_LIMIT_WAIT = 1.2


LOG = logging.getLogger(__name__)


class Form:

    def __init__(self, url):
        self.url = urllib.parse.urljoin(BASE_FORM_ENDPOINT, url)
        self.content = {
            "owner": {},
            "issuer": {},
            "transactions": {},
            "holding": {}
        }

    def __repr__(self):
        return f"Form {self.url}"

    def get_content(self):
        return self.content

    def _set_content(self, key1, key2, value):
        self.content[key1][key2] = value

    def _request_form(self):
        try:
            with request.urlopen(self.url) as url:
                url_data = url.read()
            soup = BeautifulSoup(url_data, 'html.parser')
        except urllib.error.HTTPError as e:
            LOG.exception(f'Error while downloading form: {e}')
            return None
        return soup

    async def _async_request_form(self):
        try:
            LOG.debug(f"Resuest start: {time.monotonic()}")
            await asyncio.sleep(RATE_LIMIT_WAIT)
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url) as url:
                    url_data = await url.read()
            soup = BeautifulSoup(url_data, 'html.parser')
            LOG.debug(f"Request end: {time.monotonic()}")
        except aiohttp.web.HTTPError as e:
            LOG.exception(f'Error while downloading form: {e}')
            return None

        return soup

    def _extract_owner_info(self, soup):
        """
         Find information related to reporting owner from bs4 soup.
         :param soup: bs4 soup object
         """
        reporting_owner = soup.reportingowner
        id = reporting_owner.reportingownerid
        relation = reporting_owner.reportingownerrelationship
        cik = id.rptownercik.text
        name = id.rptownername.text
        ownership_relation = []

        # Append ownership relation
        ownership_relation.append(utils.append_valid_string(relation.isdirector))
        ownership_relation.append(utils.append_valid_string(relation.isofficer))
        ownership_relation.append(utils.append_valid_string(relation.istenpercentowner))
        ownership_relation.append(utils.append_valid_string(relation.isother))
        ownership_relation.append(utils.append_valid_string(relation.officertitle))

        self._set_content('owner', 'cik', cik)
        self._set_content('owner', 'name', name)
        self._set_content('owner', 'isdirector', ownership_relation[0])
        self._set_content('owner', 'isofficer', ownership_relation[1])
        self._set_content('owner', 'istenpercentowner', ownership_relation[2])
        self._set_content('owner', 'isother', ownership_relation[3])
        self._set_content('owner', 'officertitle', ownership_relation[4])

    def _extract_transaction_info(self, soup):
        """
         Find information from nonDerivativeTransaction.
         :param soup: bs4 soup object
         """
        try:
            transactions = soup.nonderivativetable.find_all('nonderivativetransaction')
        except AttributeError:
            LOG.debug("No non derivative transactions info found.")
            raise AttributeError("No non derivative transactions info found")
        for ind, transaction in enumerate(transactions):
            security = transaction.securitytitle.text.strip()
            date = transaction.transactiondate.text.strip()
            trans_amounts = transaction.transactionamounts
            code = trans_amounts.transactionacquireddisposedcode.text.strip()
            amount = trans_amounts.transactionshares.text.strip()
            price = trans_amounts.transactionpricepershare.text.strip()
            holding_after = transaction.posttransactionamounts.sharesownedfollowingtransaction.text.strip()

            transaction = {'security': security,
                           'date': date,
                           'code': code,
                           'amount': amount,
                           'price': price,
                           'holding_after': holding_after}
            self._set_content('transactions', f"transaction{ind + 1}", transaction)

    def _extract_issuer_info(self, soup):
        """
         Find information about issuer.
         :param soup: bs4 soup object
         """
        issuer = soup.issuer
        cik = issuer.issuercik.text
        company = issuer.issuername.text
        ticker = issuer.issuertradingsymbol.text

        self._set_content('issuer', 'cik', cik)
        self._set_content('issuer', 'company', company)
        self._set_content('issuer', 'ticker', ticker)

    def _extract_holding_info(self, soup):
        """
          Find before/after holdings info.
          :param soup: bs4 soup object
          """
        holding = soup.nonderivativetable
        if holding is None:
            for field in ['holding_before', 'ownership_status', 'ownership_nature']:
                self._set_content('holding', field, '')

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

        self._set_content('holding', 'holding_before', holding_before)
        self._set_content('holding', 'ownership_status', ownership_status)
        self._set_content('holding', 'ownership_nature', ownership_nature)

    async def extract_info(self, limiter):
        async with limiter:
            soup = await self._async_request_form()
        if soup:
            self._extract_transaction_info(soup)
            self._extract_owner_info(soup)
            self._extract_issuer_info(soup)
            self._extract_holding_info(soup)
        else:
            raise AttributeError('Could not get form info.')


class Index:

    def __init__(self, url, name=None):
        if name:
            self.name = name
        else:
            self.name = url
        self.url = urllib.parse.urljoin(DAILY_INDEX_ENDPOINT, url)
        self.data = None

    def __repr__(self):
        return f"Index {self.name}"

    def __str__(self):
        return f"Index {self.name}"

    # def _decode_binary_data(self):
    #     return self.data.decode('ascii')

    def _request_index(self):
        try:
            user_agent = 'Mozilla/5.0 (iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46'
            req = request.Request(self.url, headers={'User-Agent': user_agent})
            with request.urlopen(req) as url_in:
                data = url_in.read()
        except urllib.error.HTTPError as e:
            LOG.exception(f'Error while downloading index: {e}')
            return None
        return data

    def _parse_entry(self, entry):
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

        return form, company, cik, date, filename

    def get_index(self):
        data = self._request_index()
        if data:
            self.data = data.decode('ascii')
        else:
            raise AttributeError

    def generate_form(self):
        for entry in self.data.split("\n"):
            if entry.startswith('4 ') or entry.startswith('4/A'):
            # if entry.startswith('4/A'):
            #     import pdb; pdb.set_trace()
                form_url = self._parse_entry(entry)[-1]
                form = Form(form_url)
                yield form
