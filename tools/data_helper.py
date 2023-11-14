import datetime
import pandas as pd
import pytz
from statsmodels.tsa.stattools import adfuller
import talib

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
        return pd.NA


def get_dataframe_keys(data_path):
    with pd.HDFStore(data_path) as store:
        # List all the keys/DataFrames
        dataframe_keys = store.keys()
    return dataframe_keys


def process_data(data_path, earnings_path, days_into_future=10):
    dataframe_keys = get_dataframe_keys(data_path)
    df_dict = {}
    dropped_symbols = []
    for key in dataframe_keys:
        df = pd.read_hdf(data_path, key)
        # convert time to eastern and end of trading day (4 PM EST, ignore early trading day)
        utc_time = pd.Series(df.index).apply(pytz.utc.localize)
        eastern = pytz.timezone('US/Eastern')
        eastern_time = utc_time.apply(lambda x: x.astimezone(eastern))
        new_time = datetime.time(16, 0, 0)  # Setting time to 16:00:00
        df.index = eastern_time.apply(
            lambda x: x.replace(hour=new_time.hour, minute=new_time.minute, second=new_time.second))

        # tech debt: perform this conversion when saving events to h5
        # get earnings dates
        events_df = pd.read_hdf(earnings_path, key)
        earnings_dates = events_df.loc[events_df['eventType'] == 'chartEvent/earnings', 'dateTimestamp']
        # Make it timezone aware
        utc_time = earnings_dates.apply(pytz.utc.localize)
        # Eastern timezone
        eastern = pytz.timezone('US/Eastern')
        # Convert to Eastern Time
        earnings_dates_eastern_time = utc_time.apply(lambda x: x.astimezone(eastern))

        # # drop data before earliest earnings date
        # df = df[df.index >= earnings_dates_eastern_time.min()]
        # tech debt: change to days UNTIL earnings. Requires alpha vantage to get date. Need solution for when date is unknown...
        # Apply the function to each date in df
        df['days_since_earnings'] = df.index.map(lambda date: days_since_earnings(date, earnings_dates_eastern_time))

        df['rsi'] = talib.RSI(df['close'], timeperiod=14)
        df['mfi'] = talib.MFI(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'], timeperiod=14)
        df['rmi'] = calculate_rmi(df['close'], time_period=14, momentum_period=5)

        macd = talib.MACD(
            df['close'], fastperiod=12, slowperiod=26, signalperiod=9)

        df['macd_hist'] = macd[2]
        df['close_price_diff_1_day'] = df['close'].pct_change()

        df.loc[:, 'crossover_difference'] = df['tenkan_sen'] - df['kijun_sen']
        # -1 when crossover occurs, 1 when no change of sign, otherwise 0 if crossover_difference is 0

        df.loc[:, 'crossover_indicator'] = (
            (df['crossover_difference'] * df['crossover_difference'].shift(1)).apply(
                convert_to_polarity)
        )

        # Calculate the 52-week high for each date
        # Compute the current close relative to the 52-week high
        df['close_to_365_day_high'] = df['close'] / df['close'].rolling(window='365D').max()
        # Calculate the 52-week low for each date
        # Compute the current close relative to the 52-week low
        df['close_to_365_day_low'] = df['close'] / df['close'].rolling(window='365D').min()

        dropna_cols = []
        for days in range(7, 7 * 13, 7):
            col_high = f'close_to_{days}_day_high'
            col_low = f'close_to_{days}_day_low'
            df[col_high] = df['close'] / df['close'].rolling(window=f'{days}D').max()
            df[col_low] = df['close'] / df['close'].rolling(window=f'{days}D').min()
            dropna_cols.extend([col_high, col_low])

        # (close - feature) / feature from Ichimoku cloud
        df.loc[:, 'close_diff_tenkan_sen_percent'] = (df['close'] - df['tenkan_sen']) / df['tenkan_sen']
        df.loc[:, 'close_diff_kijun_sen_percent'] = (df['close'] - df['kijun_sen']) / df['kijun_sen']
        df.loc[:, 'close_diff_senkou_span_a_percent'] = (df['close'] - df['senkou_span_a']) / df['senkou_span_a']
        df.loc[:, 'close_diff_senkou_span_b_percent'] = (df['close'] - df['senkou_span_b']) / df['senkou_span_b']

        # Introduce seasonality
        df.loc[:, 'month'] = df.index.month

        target_col = f'highest_close_next_{days_into_future}_days_percent'
        # df.loc[:, target_col] = df['close'].pct_change(offset)
        df[target_col] = (
                                 df['close'].shift(-days_into_future).rolling(window=days_into_future,
                                                                              min_periods=days_into_future).max() - df[
                                     'close']
                         ) / df['close']

        # highest_close_col = f'highest_close_next_{days_into_future}_days_percent'
        # # df.loc[:, target_col] = df['close'].pct_change(offset)
        # df[highest_close_col] = (
        #     df['close'].shift(-days_into_future).rolling(window=days_into_future, min_periods=days_into_future).max() - df['close']
        # ) / df['close']
        #
        # lowest_close_col = f'lowest_close_next_{days_into_future}_days_percent'
        # # df.loc[:, target_col] = df['close'].pct_change(offset)
        # df[lowest_close_col] = (
        #     df['close'].shift(-days_into_future).rolling(window=days_into_future, min_periods=days_into_future).min() - df['close']
        # ) / df['close']

        # long_idx = (
        #     (df['crossover_indicator'] == -1) & (df['crossover_difference'] > 0) &
        #     (df['close_diff_senkou_span_a_percent'] > 0) & (df['close_diff_senkou_span_b_percent'] > 0)
        # )
        # short_idx = (
        #     (df['crossover_indicator'] == -1) & (df['crossover_difference'] < 0) &
        #     (df['close_diff_senkou_span_a_percent'] < 0) & (df['close_diff_senkou_span_b_percent'] < 0)
        # )
        #
        # target_col = f'signal_close_next_{days_into_future}_days_percent'
        # df.loc[:, target_col] = pd.NA
        # df.loc[long_idx, target_col] = df.loc[long_idx, highest_close_col]
        # df.loc[short_idx, target_col] = df.loc[short_idx, lowest_close_col]



        # df = df.loc[long_idx | short_idx].copy()
        df = df.dropna(subset=[target_col]).copy()
        if df.shape[0] > 0:
            # Check if p-value is <= 0.05 to confirm stationary series
            result = adfuller(df[target_col])
        else:
            dropped_symbols.append(key)
            continue
        if result[1] < 0.05:
            ### Train when buliish crossover signal is present
            # If crossover_indicator == -1
            # and crossover_difference > 0
            # and signal closes above cloud
            # then LONG signal

            long_idx = ((df['crossover_indicator'] == -1) & (df['crossover_difference'] > 0) &
                        (df['close_diff_senkou_span_a_percent'] > 0) & (df['close_diff_senkou_span_b_percent'] > 0))

            df = df.loc[long_idx].copy()
            df.drop(['open', 'high', 'low', 'close', 'volume', 'dividend_amount',
                     'tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b',
                     'chikou_span',  # not a known value in real-time.
                     ], axis=1, inplace=True)
            df_dict[key] = df.dropna(subset=[
                                                'close_price_diff_1_day', 'crossover_indicator',
                                                'close_diff_tenkan_sen_percent', 'close_diff_kijun_sen_percent',
                                                'close_diff_senkou_span_a_percent', 'close_diff_senkou_span_b_percent',
                                                'rsi', 'rmi', 'mfi', 'macd_hist', 'days_since_earnings',
                                                'close_to_365_day_high', 'close_to_365_day_low',
                                                target_col,
                                            ] + dropna_cols).copy()
        else:
            dropped_symbols.append(key)
            print('p-value: %f' % result[1])
            print(f"Dropped {key} because it did not pass the stationary series test")\

    return df_dict, dropped_symbols