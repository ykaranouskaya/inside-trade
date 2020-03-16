"""
Merge all data pieces into a single dataframe, that can be than used for modeling.
"""
from datetime import datetime, timedelta
import json
from pathlib import Path

import pandas as pd

from insider_trading.preprocess import feature_engineering as feat_eng


def json_to_csv(symbol, data_root):
    """
    Load market data from a .json file and convert it into a data frame.
    :param symbol: string, ticker symbol
    :param data_root: path to the data location
    :return: data frame
    """
    with open((Path(data_root) / (symbol + '.json')), 'r') as f:
        json_data = json.load(f)

    # output dataframe formatting
    dict_data = {'date': [],
                 'open': [],
                 'close': [],
                 'high': [],
                 'low': [],
                 'volume': [],
                 'adjusted close': [],
                 'dividend amount': []}

    json_data = json_data['Weekly Adjusted Time Series']
    for date in json_data:
        dict_data['date'].append(date)
        for item in json_data[date]:
            key = item[3:]
            dict_data[key].append(float(json_data[date][item]))
    csv_data = pd.DataFrame(data=dict_data)

    # sort by date
    csv_data.sort_values(by='date', inplace=True)
    csv_data['date'] = csv_data['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    csv_data['TICKER'] = symbol

    return csv_data


def merge_forms_market(forms_csv, market_root, ma_windows=[],
                       ma_cols=['adjusted close', 'adjusted high', 'adjusted low'],
                       add_sp500=True):
    """
    Merge form filings data with market data.
    Adds adjusted high and low, keeps all other columns from market data.
    :param forms_csv: path to the csv file storing forms filing info.
    :param market_root: path to the folder storing market data.
    :param ma_windows: moving average windows to add
    :param ma_cols: columns to compute moving average for
    :param add_sp500: boolean, include S&P500 benchmark or not
    :return: merged data frame
    """

    # load forms data
    forms_df = pd.read_csv(forms_csv)
    forms_symb = set(forms_df['TICKER'])

    # drop duplicated
    forms_df.drop_duplicates(inplace=True)

    # load market data
    market_df = []

    for symb in forms_symb:
        try:
            df = json_to_csv(symb, market_root)
        except Exception as e:
            print(f"Error loading {symb}: {e}")
            continue
        feat_eng.add_adjusted(df)
        for window in ma_windows:
            feat_eng.add_ma(df, ma_cols, window)
        market_df.append(df)

    market_df = pd.concat(market_df)
    market_df.sort_values(by='date', inplace=True)

    # Parse dates
    forms_df['REPORT_DATE'] = pd.to_datetime(forms_df['REPORT_DATE'])

    # merge
    merged_df = pd.merge_asof(forms_df, market_df,
                              by='TICKER', left_on='REPORT_DATE', right_on='date',
                              tolerance=pd.Timedelta(days=7))

    if add_sp500:
        df = json_to_csv('SPX', market_root)
        feat_eng.add_ma(df, ['adjusted close'], window=4)
        df.rename(columns={'adjusted close': 'spx_adjusted_close',
                           'adjusted close_ma_4': 'spx_adjusted_close_ma_4',
                           'date': 'spx_date'}, inplace=True)
        df.sort_values(by='spx_date', inplace=True)
        merged_df = pd.merge_asof(merged_df, df[['spx_date', 'spx_adjusted_close', 'spx_adjusted_close_ma_4']],
                                  left_on='REPORT_DATE', right_on='spx_date',
                                  tolerance=pd.Timedelta(days=7))

    # drop nans
    merged_df = merged_df[~merged_df['date'].isnull()]

    return merged_df


if __name__ == "__main__":
    forms_csv = './data/2019/database.csv'
    market_root = './data/market_data/'

    merged_df = merge_forms_market(forms_csv, market_root)