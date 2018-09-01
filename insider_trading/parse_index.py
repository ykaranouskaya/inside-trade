# !usr/bin/python

import datetime
import re
import xml.etree.cElementTree as cElementTree
import urllib.request as request
from pathlib import Path


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



if __name__ == "__main__":
     # url = DAILY_INDEX_ENDPOINT + '2018/QTR3/sitemap.20180824.xml'
     form = Path('./data') / 'form.20180824.idx'

     for entry in read_form_index_entries(form):
          print(entry)