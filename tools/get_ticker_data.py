from alpha_vantage.timeseries import TimeSeries
import os
import pandas as pd
import psycopg2
import time
from tools import get_daily_adjusted_processed, calculate_ichimoku
from tools.database_helper import get_ticker_id


def main(symbols, path=None, table='stock_quotes_daily', save_type='psql', outputsize='full'):

    if save_type == 'hdf5':
        index_etfs = ['SPY', 'QQQ', 'DIA', 'IWM']
        download_and_save_hdf5(index_etfs, path, 'indices')
        download_and_save_hdf5(symbols, path, 'prices')
    elif save_type == 'psql':
        download_and_save_sql(
            symbols, table, dbname="stock", host="localhost",
            outputsize=outputsize, sleep_time=0.1
        )
    else:
        raise Exception('Unknown save type. Choose from "hdf5" or "psql"')


def download_and_save_hdf5(symbols, path, directory, sleep_time=0.1, outputsize='full'):
    for symbol in symbols:
        print(symbol)
        # get technical indicators
        ts = TimeSeries(key=os.environ.get('ALPHAVANTAGE_API_KEY'), output_format='pandas')
        data, meta_data = ts.get_daily_adjusted(symbol=symbol, outputsize=outputsize)
        data = get_daily_adjusted_processed(data)
        data = calculate_ichimoku(data)
        # Create an HDF5 file (if it doesn't exist) and open it in append mode
        with pd.HDFStore(path, mode='a') as store:
            # Save each DataFrame to the store
            store.put(f'{directory}/{symbol}', data, format='table', data_columns=True)
        time.sleep(sleep_time)


def download_and_save_sql(symbols, table, reference_table='tickers', dbname="stock",
                          host="localhost", outputsize='full', sleep_time=0.1):
    # Connect to your postgres DB
    conn = psycopg2.connect(
        dbname=dbname,
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host=host
    )

    # Open a cursor to perform database operations
    cur = conn.cursor()

    query = """
        INSERT INTO {} (ticker_id, date, open, high, low, close, adjusted_close, volume, dividend_amount, split_coefficient)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker_id, date) DO NOTHING;
        """.format(table)

    for ticker_symbol in symbols:
        print(ticker_symbol)
        ticker_id = get_ticker_id(cur, ticker_symbol, reference_table=reference_table)


        ts = TimeSeries(key=os.environ.get('ALPHAVANTAGE_API_KEY'), output_format='pandas')
        data, meta_data = ts.get_daily_adjusted(symbol=ticker_symbol, outputsize=outputsize)
        # data = get_daily_adjusted_processed(data)
        # data = calculate_ichimoku(data)

        # Insert data into PostgreSQL database
        for index, row in data.iterrows():
            cur.execute(
                query,
                (ticker_id, index, row['1. open'], row['2. high'], row['3. low'], row['4. close'],
                 row['5. adjusted close'],
                 row['6. volume'], row['7. dividend amount'], row['8. split coefficient'])
            )
        conn.commit()
        time.sleep(sleep_time)
