"""
Create a simple benchmark that uses data with minimal preprocessing, using random forest model.
"""
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

from insider_trading.preprocess import feature_engineering as feat_eng
from insider_trading.config import *


def prepare_data(data_path):
    """
    Load the data and perform basic preprocessing.
    :param data_path: path to the merged data csv file.
    :return: prepared daataframe.
    """
    # Load data
    df = pd.read_csv(data_path)

    # Apply shifts
    ma_cols = df.filter(regex=f'{ADJUSTED_CLOSE}_ma_\d').columns.to_list()
    df = feat_eng.add_shifted(df, cols=[ADJUSTED_CLOSE, *ma_cols,
                              SPX_ADJUSTED_CLOSE, f'{SPX_ADJUSTED_CLOSE}_ma_4'], dt_days=180)

    # Compute relative gains
    ma_ = ma_cols[0]
    df = feat_eng.add_gains(df, new_col=f'{ma_}_180', ref_col=ma_,
                            new_col_name=f'change_{ma_}')
    df = feat_eng.add_gains(df, new_col=f'{SPX_ADJUSTED_CLOSE}_ma_4_180', ref_col=f'{SPX_ADJUSTED_CLOSE}_ma_4',
                            new_col_name=f'change_{SPX_ADJUSTED_CLOSE}_ma_4')

    # SPX benchmark gains
    df[SPX_GAIN] = df[f'change_{ma_},%'] - df[f'change_{SPX_ADJUSTED_CLOSE}_ma_4,%']

    # Clean and Validate
    df = feat_eng.drop_zeros(df)
    df = feat_eng.process_ppu_outliers(df)
    df = feat_eng.validate_ppu_to_market(df)

    # Compute relative holdings change
    df = feat_eng.add_holding_change_perc(df, new_col=HOLDING_CHANGE, cap=True)

    # Handle booleans
    df = feat_eng.process_booleans(df)
    df = feat_eng.process_aquired(df)
    df = feat_eng.process_direct_ownership(df)

    return df


def prepare_train_test(df, target_cols=[], features_cols=[],
                       test_size=0.2, shuffle=True, random_seed=RANDOM_SEED):
    """
    Prepare data for training.
    :param df: dataframe
    :param target_cols: list of target columns
    :param features_cols: list of features columns
    :param test_size: test set ratio, default: 0.2
    :param shuffle: boolean, weather to shuffle data before split
    :param random_seed: random state
    :return: (train_data, test_data) tuple, where each entry is (X, y) data tuple
    """
    # Drop NaNs
    df = df[features_cols + target_cols].dropna()

    y = df[target_cols].values
    features = df[features_cols].values

    X_train, X_test, y_train, y_test = train_test_split(features, y, test_size=test_size, shuffle=shuffle,
                                                        random_state=random_seed)
    train_data = (X_train, y_train)
    test_data = (X_test, y_test)

    return train_data, test_data


def rf_benchmark(train_data, **model_params):
    """
    Train and test random forest model
    :param train_data: (X, y) train data
    :param test_data: (X, y) test data
    :param model_params: parameters of the model
    :return model: trained model
    """
    rf_model = RandomForestRegressor(**model_params)
    rf_model.fit(*train_data)

    return rf_model


def evaluate(model, data, plot=True):

    X, y = data

    # Get model score
    score = model.score(X, y)
    print(f"Score: {score}")

    pred = model.predict(X)
    mse = mean_squared_error(y, pred)
    mae = mean_absolute_error(y, pred)
    print(f"MSE: {mse}")
    print(f"MAE: {mae}")

    if plot:
        fig = plt.figure(figsize=(10, 10))
        plt.scatter(y, pred)
        plt.xlabel('Target')
        plt.ylabel('Prediction')
