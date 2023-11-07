from tools import get_daily_adjusted_processed, calculate_ichimoku
from alpha_vantage.timeseries import TimeSeries
import os
import pandas as pd
import time


sandp500_df = pd.read_csv('res/indices/s_and_p_500_details.csv')

for symbol in sandp500_df['Ticker']:
    print(symbol)
    # get technical indicators
    ts = TimeSeries(key=os.environ['ALPHAVANTAGE_API_KEY'], output_format='pandas')
    data, meta_data = ts.get_daily_adjusted(symbol=symbol, outputsize='full')
    data = get_daily_adjusted_processed(data)
    data = calculate_ichimoku(data)
    # Create an HDF5 file (if it doesn't exist) and open it in append mode
    with pd.HDFStore('res/data/s_and_p_data.h5', mode='a') as store:
        # Save each DataFrame to the store
        store.put(symbol, data, format='table', data_columns=True)
    time.sleep(0.1)