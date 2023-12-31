import datetime
import numpy as np
import os
import pandas as pd
import pytz
from statsmodels.tsa.stattools import adfuller
import talib

from tools.json_helper import load_dict_from_json
from tools.pattern_helper import convert_to_polarity, calculate_rmi


def days_since_earnings(date, earnings_dates):
    """Function to get days since last earnings date"""
    # TECH DEBT: confirm ET in earnings dates
    # TECH DEBT: need total trading days instead of calendar days.
    past_earnings_dates = earnings_dates[earnings_dates <= date]
    if not past_earnings_dates.empty:
        last_earnings_date = past_earnings_dates.max()
        return (date - last_earnings_date).days + 1
    else:
        return np.NaN


def get_dataframe_keys(data_path):
    with pd.HDFStore(data_path) as store:
        # List all the keys/DataFrames
        dataframe_keys = store.keys()
    return dataframe_keys


def make_index_eastern(df):
    """Convert time to eastern and end of trading day (4 PM EST, ignore early trading day)"""
    utc_time = pd.Series(df.index).apply(pytz.utc.localize)
    eastern = pytz.timezone('US/Eastern')
    eastern_time = utc_time.apply(lambda x: x.astimezone(eastern))
    new_time = datetime.time(16, 0, 0)  # Setting time to 16:00:00
    df.index = eastern_time.apply(
        lambda x: x.replace(hour=new_time.hour, minute=new_time.minute, second=new_time.second)
    )
    return df


def get_earnings_dates(df):
    earnings_dates = df.loc[df['eventType'] == 'chartEvent/earnings', 'dateTimestamp']
    # Make it timezone aware
    utc_time = earnings_dates.apply(pytz.utc.localize)
    # Eastern timezone
    eastern = pytz.timezone('US/Eastern')
    # Convert to Eastern Time
    earnings_dates_eastern_time = utc_time.apply(lambda x: x.astimezone(eastern))

    return earnings_dates_eastern_time


def process_data(data_path, number_of_shifts=13, spy_number_of_shifts=13, shift_step=10):
    week_multiplier = 2
    high_low_rolling_calendar_days = range(week_multiplier * 7, 13 * week_multiplier * 7, week_multiplier * 7)

    dropna_cols = [
        'close_price_diff_1_day', 'crossover_indicator',
        'rsi', 'rmi', 'mfi', 'macd', 'macd_signal', 'macd_hist', 'days_since_earnings',
        'close_to_365_day_high', 'close_to_365_day_low',
        'volume_percent_of_2_week_total', 'dividend_amount_to_close',
    ]

    # Use reduced data file for testing
    if os.environ.get('TEST_ENV') == 'true':
        data_path = '../../../res/data/s_and_p_study_data_TESTING.h5'

    s_and_p_details = pd.read_csv('../../../res/indices/s_and_p_500_details.csv', index_col=0)

    dataframe_keys = get_dataframe_keys(data_path)
    prices_dataframe_keys = [k for k in dataframe_keys if 'prices/' in k]
    events_dataframe_keys = [k for k in dataframe_keys if 'events/' in k]
    index_dataframe_keys = [k for k in dataframe_keys if 'indices/' in k]

    index_dfs = []
    ind_features = []
    for ind in ['SPY', 'QQQ', 'DIA']:

        ind_df = pd.read_hdf(data_path, f'indices/{ind}')
        ind_df = make_index_eastern(ind_df)

        ind_df.loc[:, f'{ind}_close_diff_tenkan_sen_percent'] = (ind_df['close'] - ind_df['tenkan_sen']) / ind_df['tenkan_sen']
        ind_df.loc[:, f'{ind}_close_diff_kijun_sen_percent'] = (ind_df['close'] - ind_df['kijun_sen']) / ind_df['kijun_sen']
        ind_df.loc[:, f'{ind}_close_diff_senkou_span_a_percent'] = (ind_df['close'] - ind_df['senkou_span_a']) / ind_df[
            'senkou_span_a']
        ind_df.loc[:, f'{ind}_close_diff_senkou_span_b_percent'] = (ind_df['close'] - ind_df['senkou_span_b']) / ind_df[
            'senkou_span_b']

        features = [
                f'{ind}_close_diff_tenkan_sen_percent', f'{ind}_close_diff_kijun_sen_percent',
                f'{ind}_close_diff_senkou_span_a_percent', f'{ind}_close_diff_senkou_span_b_percent'
            ]

        ind_features.extend(features)

        ind_df, shift_features = add_shifted_columns(ind_df, features, spy_number_of_shifts, shift_step=shift_step)
        ind_features.extend(shift_features)

        index_dfs.append(ind_df.copy())

    # Initialize merged_df with the first dataframe
    ind_df = index_dfs[0]

    # Loop through the remaining dataframes and merge
    for df_temp in index_dfs[1:]:
        ind_df = pd.merge(ind_df, df_temp, left_index=True, right_index=True, how='outer')

    df_dict = {}
    dropped_symbols = []

    for key in prices_dataframe_keys:
        # print(key)
        symbol = key.split('/')[-1]
        df = pd.read_hdf(data_path, key)
        df = make_index_eastern(df)

        df = df.merge(ind_df[ind_features], left_index=True, right_index=True, how='left')

        # tech debt: perform this conversion when saving events to h5
        # get earnings dates
        events_key = f'/events/{symbol}'
        if events_key in events_dataframe_keys:
            events_df = pd.read_hdf(data_path, events_key)
            earnings_dates_eastern_time = get_earnings_dates(events_df)
        else:
            dropped_symbols.append(key)
            print(f"Dropped {key} because it did not have event data.")
            continue

        # tech debt: change to days UNTIL earnings. Requires alpha vantage to get date. Need solution for when date is unknown...
        # Apply the function to each date in df
        df['days_since_earnings'] = df.index.map(lambda date: days_since_earnings(date, earnings_dates_eastern_time))
        df['days_since_earnings'].astype(float)

        # introduce sector info, need to make one-hots
        df['sector'] = s_and_p_details.Sector.get(symbol, 'UNKNOWN')

        # Introduce seasonality
        df.loc[:, 'month'] = df.index.month
        df['month'] = df['month'].astype(float)

        # Technical Indicators
        df['rsi'] = talib.RSI(df['close'], timeperiod=14)
        df['mfi'] = talib.MFI(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'], timeperiod=14)
        df['rmi'] = calculate_rmi(df['close'], time_period=14, momentum_period=5)

        macd = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        df['macd'] = macd[0]
        df['macd_signal'] = macd[1]
        df['macd_hist'] = macd[2]

        df['close_price_diff_1_day'] = df['close'].pct_change()

        df.loc[:, 'crossover_difference'] = df['tenkan_sen'] - df['kijun_sen']
        # -1 when crossover occurs, 1 when no change of sign, otherwise 0 if crossover_difference is 0
        df.loc[:, 'crossover_indicator'] = (
            (df['crossover_difference'] * df['crossover_difference'].shift(1)).apply(
                convert_to_polarity)
        )

        # Relative Volume
        df['volume_percent_of_2_week_total'] = 100 * df['volume'] / df['volume'].rolling(window='14D').sum()
        # Relative Dividend
        df['dividend_amount_to_close'] = 100 * df['dividend_amount'] / df['close']

        # (close - feature) / feature from Ichimoku cloud
        df.loc[:, 'close_diff_tenkan_sen_percent'] = (df['close'] - df['tenkan_sen']) / df['tenkan_sen']
        df.loc[:, 'close_diff_kijun_sen_percent'] = (df['close'] - df['kijun_sen']) / df['kijun_sen']
        df.loc[:, 'close_diff_senkou_span_a_percent'] = (df['close'] - df['senkou_span_a']) / df['senkou_span_a']
        df.loc[:, 'close_diff_senkou_span_b_percent'] = (df['close'] - df['senkou_span_b']) / df['senkou_span_b']

        cloud_features = ['close_diff_tenkan_sen_percent', 'close_diff_kijun_sen_percent',
                        'close_diff_senkou_span_a_percent', 'close_diff_senkou_span_b_percent']
        dropna_cols.extend(cloud_features)

        df, shift_cloud_features = add_shifted_columns(df, cloud_features, number_of_shifts, shift_step=shift_step)
        dropna_cols.extend(shift_cloud_features)

        # Calculate the 52-week high for each date
        # Compute the current close relative to the 52-week high
        df['close_to_365_day_high'] = df['close'] / df['close'].rolling(window='365D').max()
        # Calculate the 52-week low for each date
        # Compute the current close relative to the 52-week low
        df['close_to_365_day_low'] = df['close'] / df['close'].rolling(window='365D').min()

        for days in high_low_rolling_calendar_days:
            col_high = f'close_to_{days}_day_high'
            col_low = f'close_to_{days}_day_low'
            df[col_high] = df['close'] / df['close'].rolling(window=f'{days}D').max()
            df[col_low] = df['close'] / df['close'].rolling(window=f'{days}D').min()
            dropna_cols.extend([col_high, col_low])

        if df.shape[0] > 0:
            df_dict[key] = df.dropna(
                subset=dropna_cols + ind_features
            ).copy()
        else:
            dropped_symbols.append(key)
            print(f"Dropped {key} because it is an empty dataframe")
    return df_dict, dropped_symbols


def filter_data(df, signal_rule='bullish_cloud_crossover'):
    idx = get_signal_index(df, signal_rule=signal_rule)

    return df.loc[idx].copy()


def get_signal_index(df, signal_rule):
    if signal_rule == 'bullish_cloud_crossover':
        # Train when bullish crossover signal is present
        # If crossover_indicator == -1
        # and crossover_difference > 0
        # and signal closes above cloud
        # then LONG signal
        idx = (
                (df['crossover_indicator'] == -1) &
                (df['crossover_difference'] > 0) &
                (df['close_diff_senkou_span_a_percent'] > 0) &
                (df['close_diff_senkou_span_b_percent'] > 0)
        )
    elif signal_rule == 'bearish_cloud_crossover':
        # Train when bearish crossover signal is present
        # If crossover_indicator == -1
        # and crossover_difference < 0
        # and signal closes below cloud
        # then SHORT signal
        idx = (
                (df['crossover_indicator'] == -1) &
                (df['crossover_difference'] < 0) &
                (df['close_diff_senkou_span_a_percent'] < 0) &
                (df['close_diff_senkou_span_b_percent'] < 0)
        )
    elif signal_rule == 'bullish_and_bearish_cloud_crossover':
        # Need to determine target
        idx = (
                (
                        (df['crossover_indicator'] == -1) &
                        (df['crossover_difference'] > 0) &
                        (df['close_diff_senkou_span_a_percent'] > 0) &
                        (df['close_diff_senkou_span_b_percent'] > 0)
                ) |
                (
                        (df['crossover_indicator'] == -1) &
                        (df['crossover_difference'] < 0) &
                        (df['close_diff_senkou_span_a_percent'] < 0) &
                        (df['close_diff_senkou_span_b_percent'] < 0)
                )
        )
    else:
        raise Exception(f'Unknown signal rule {signal_rule}')
    return idx


def add_shifted_columns(df, column_names, number_of_shifts, shift_step=1):
    """
    Adds shifted columns to the DataFrame for multiple columns with a specified shift step.

    :param df: Pandas DataFrame
    :param column_names: List of column names to be shifted
    :param number_of_shifts: Number of shifted columns to add for each column
    :param shift_step: The step size for each shift
    :return: DataFrame and List of newly added column names
    """
    new_columns = []
    for column_name in column_names:
        for n in range(1, number_of_shifts + 1):
            shift_amount = n * shift_step
            shifted_column_name = f"{column_name}_shifted_{shift_amount}"
            df[shifted_column_name] = df[column_name].shift(shift_amount)
            new_columns.append(shifted_column_name)
    return df.copy(), new_columns


def evaluate_for_stationary_series(target: pd.Series, test_threshold=0.05):
    result = adfuller(target)

    return result[1] < test_threshold

if __name__ == '__main__':
    process_data('../../../res/data/s_and_p_study_data_TESTING.h5')
