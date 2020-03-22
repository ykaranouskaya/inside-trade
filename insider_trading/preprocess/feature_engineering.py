"""
Utility functions for feature engineering.
"""
from datetime import timedelta

import numpy as np
import pandas as pd

from insider_trading.config import *

DATE_FORMAT = '%Y-%m-%d'


def add_adjusted(df):
    """
    Compute adjusted high and low from `adjusted close`, works inplace.
    """
    df[ADJUSTED_HIGH] = df[ADJUSTED_CLOSE] + df[HIGH] - df[CLOSE]
    df[ADJUSTED_LOW] = df[ADJUSTED_CLOSE] + df[LOW] - df[CLOSE]


def add_ma(df, cols=[ADJUSTED_CLOSE, ADJUSTED_HIGH, ADJUSTED_LOW],
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
    df_shifted[DATE] = pd.to_datetime(df_shifted[DATE], format=DATE_FORMAT)
    df_shifted[SHIFTED_DATE] = df_shifted[DATE].apply(lambda x: x - tdelta)

    return df_shifted


def add_shifted(df, date_col=DATE, cols=[ADJUSTED_CLOSE], dt_days=180):
    """
    Add `dt_days` shifted data from `cols` to the dataframe.
    :param df: dataframe
    :param date_col: column name of date to shift
    :param cols: columns to shift, excluding date column
    :param dt_days: timedelta, how much time backward to shift
    :return: dataframe with added columns
    """
    shifted_cols = cols + [TICKER]
    df_shifted = shift_price_date(df, tdelta=timedelta(days=dt_days), cols=shifted_cols+[date_col])
    df_shifted.rename(columns={col: col + f'_{dt_days}' for col in cols}, inplace=True)

    df_new = df.copy()
    df_new[REPORT_DATE] = pd.to_datetime(df[REPORT_DATE], format=DATE_FORMAT)

    df_shifted.drop(DATE, axis=1, inplace=True)
    df_shifted = df_shifted.sort_values(by=SHIFTED_DATE)
    df_new = pd.merge_asof(df_new, df_shifted, by=TICKER, left_on=REPORT_DATE, right_on=SHIFTED_DATE,
                           tolerance=pd.Timedelta(days=7))

    df_new = df_new[~df_new[SHIFTED_DATE].isnull()]
    df_new.drop(SHIFTED_DATE, axis=1, inplace=True)

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
    buy = df[AQUIRED] == 'A'
    sell = df[AQUIRED] == 'D'

    change_col = f'{new_col_name}'
    change_col_rel = f'{new_col_name},%'

    df[change_col] = df[new_col] - df[ref_col]
    df[sell][change_col] *= -1

    if perc:
        df[change_col_rel] = (df[new_col] - df[ref_col]) / df[ref_col]
        df[sell][change_col_rel] *= -1

    return df


def process_ppu_outliers(df, thr=6000., tol=0.5, replace=True):
    """
    Remove report price outliers.

    In particular, all prices higher than `thr` are compared to the actual market adjusted close price.
    If relative difference is greater `tol`, values are replaced with NaNs or market price is `replace` is True.
    Otherwise, prices are devided by amount value to double force unit price.
    :param df: dataframe
    :param thr: price threshold. All reported prices higher than this threshold are compared to market prices.
    :param tol: tolerance ratio. If reported price and market price difference
    :return:
    """
    df_slice = df[df[PRICE_PER_UNIT] > thr]
    df_slice['NORM_PPU'] = df_slice[PRICE_PER_UNIT] / df_slice[AMOUNT]

    tol_cond = abs(df_slice['NORM_PPU'] - df_slice[ADJUSTED_CLOSE]) / df_slice['NORM_PPU'] < tol
    ids = df_slice.index[tol_cond]
    nan_ids = df_slice.index[~tol_cond]

    data = df.copy()
    data.loc[ids, PRICE_PER_UNIT] = df_slice.loc[ids]['NORM_PPU']
    if replace:
        data.loc[nan_ids, PRICE_PER_UNIT] = df_slice.loc[nan_ids][ADJUSTED_CLOSE]
    else:
        data.loc[nan_ids, PRICE_PER_UNIT] = np.nan

    return data


def validate_ppu_to_market(df, max_ratio=10):
    ratio = df[PRICE_PER_UNIT] / df[ADJUSTED_CLOSE]

    df = df[ratio < max_ratio]
    df = df[ratio > (1. / max_ratio)]

    return df


def drop_zeros(df, column=PRICE_PER_UNIT):
    df = df[df[column] != 0.]

    return df


def add_holding_change_perc(df, new_col='HOLDING_CHANGE,%', cap=False):
    """
    Compute fractional change in holdings after transaction.
    :param df: dataframe
    :param new_col: name of new column
    :param cap: bool, if True, cap all large values to 1.
    :return: dataframe with additional holding change column
    """
    df[new_col] = df[AMOUNT] / df[HOLDING_BEFORE]
    df[new_col].loc[df[AMOUNT] == 0] = 0
    df[new_col].loc[(df[AMOUNT] != 0) & (df[HOLDING_BEFORE] == 0)] = 1.

    if cap:
        large = df[new_col] > 1.
        df[new_col].loc[large] = 1.

    return df


def _replace_bools(x):
    if x == 'true' or x == '1':
        return 1
    elif x == 'false' or x == '0':
        return 0

    return x


def process_booleans(df):
    """
    Format booleans as 0/1 integers.
    :param df: dataframe
    :return: processed dataframe
    """
    boolean_cols = [IS_DIRECTOR, IS_OFFICER, IS_MAJOR_OWNER, IS_OTHER]

    df[boolean_cols] = df[boolean_cols].applymap(_replace_bools)

    return df