from alpha_vantage.timeseries import TimeSeries
import os
import pandas as pd
import time
from tools import get_daily_adjusted_processed, calculate_ichimoku


def main(path):
    sandp500_df = pd.read_csv('res/indices/s_and_p_500_details.csv')

    download_and_save(['SPY', 'QQQ', 'DIA'], path, 'indices')
    download_and_save(sandp500_df['Ticker'], path, 'prices')


def download_and_save(symbols, path, directory):
    for symbol in symbols:
        print(symbol)
        # get technical indicators
        ts = TimeSeries(key=os.environ['ALPHAVANTAGE_API_KEY'], output_format='pandas')
        data, meta_data = ts.get_daily_adjusted(symbol=symbol, outputsize='full')
        data = get_daily_adjusted_processed(data)
        data = calculate_ichimoku(data)
        # Create an HDF5 file (if it doesn't exist) and open it in append mode
        with pd.HDFStore(path, mode='a') as store:
            # Save each DataFrame to the store
            store.put(f'{directory}/{symbol}', data, format='table', data_columns=True)
        time.sleep(0.1)
