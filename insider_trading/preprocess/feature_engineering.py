"""
Utility functions for feature engineering.
"""
from datetime import timedelta

import pandas as pd

DATE_FORMAT = '%Y-%m-%d'


def add_adjusted(df):
    """
    Compute adjusted high and low from `adjusted close`, works inplace.
    """
    df['adjusted high'] = df['adjusted close'] + df['high'] - df['close']
    df['adjusted low'] = df['adjusted close'] + df['low'] - df['close']


def add_ma(df, cols=['adjusted close', 'adjusted high', 'adjusted low'],
           window=4, shift=True):
    """
    Add moving average for each column in `cols`.
    If `shift` is True, shifts MA columns backward by window / 2.
    """
    for c in cols:
        c_name = f'{c}_ma_{window}'
        df[c_name] = df[c].rolling(window=window).mean()
        if shift:
            df[c_name] = df[c_name].shift(-window // 2)


def shift_price_date(df, tdelta=timedelta(days=180), cols=None):
    """
    Add column with shifted by `tdelta` date.
    :param df: data frame with `date` column
    :param tdelta: Timedelta, how much backward shift to apply
    :param cols: list, column names of columns to keep in the returned dataframe
    :return: dataframe
    """
    if cols is None:
        cols = df.columns

    df_shifted = df[cols].copy()
    df_shifted['date'] = pd.to_datetime(df_shifted['date'], format=DATE_FORMAT)
    df_shifted['shifted_date'] = df_shifted['date'].apply(lambda x: x - tdelta)

    return df_shifted


def add_shifted(df, date_col='date', cols=['adjusted close'], dt_days=180):
    """
    Add `dt_days` shifted data from `cols` to the dataframe.
    :param df: dataframe
    :param date_col: column name of date to shift
    :param cols: columns to shift, excluding date column
    :param dt_days: timedelta, how much time backward to shift
    :return: dataframe with added columns
    """
    shifted_cols = cols + ['TICKER']
    df_shifted = shift_price_date(df, tdelta=timedelta(days=dt_days), cols=shifted_cols+[date_col])
    df_shifted.rename(columns={col: col + f'_{dt_days}' for col in cols}, inplace=True)

    df_new = df.copy()
    df_new['REPORT_DATE'] = pd.to_datetime(df['REPORT_DATE'], format=DATE_FORMAT)

    df_shifted.drop('date', axis=1, inplace=True)
    df_shifted = df_shifted.sort_values(by='shifted_date')
    df_new = pd.merge_asof(df_new, df_shifted, by='TICKER', left_on='REPORT_DATE', right_on='shifted_date',
                           tolerance=pd.Timedelta(days=7))

    df_new = df_new[~df_new['shifted_date'].isnull()]
    df_new.drop('shifted_date', axis=1, inplace=True)

    return df_new


def add_gains(df, new_col='adjusted close_ma_4_180', ref_col='adjusted close_ma_4',
              new_col_name='change_adj_close_ma_4', perc=True):
    """
    Compute absolute and relative gains.
    :param df: data frame
    :param new_col: changed column
    :param ref_col: reference column
    :return: data frame with new columns `change_*` and `change_*,%`
    """
    buy = df['AQUIRED/DISPOSED'] == 'A'
    sell = df['AQUIRED/DISPOSED'] == 'D'

    change_col = f'{new_col_name}'
    change_col_rel = f'{new_col_name},%'

    df[change_col] = df[new_col] - df[ref_col]
    df[sell][change_col] *= -1

    if perc:
        df[change_col_rel] = (df[new_col] - df[ref_col]) / df[ref_col]
        df[sell][change_col_rel] *= -1

    return df
