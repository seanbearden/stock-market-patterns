from alpha_vantage.timeseries import TimeSeries
import os
import pandas as pd
import psycopg2
import time
from tools import get_daily_adjusted_processed, calculate_ichimoku
from tools.database_helper import get_ticker_id


def main(symbols, path=None, table_prefix='stock_quotes', save_type='psql', outputsize='full',
         conn=None, cur=None):

    if save_type == 'hdf5':
        index_etfs = ['SPY', 'QQQ', 'DIA', 'IWM']
        download_and_save_hdf5(index_etfs, path, 'indices')
        download_and_save_hdf5(symbols, path, 'prices')
    elif save_type == 'psql':
        download_and_save_daily_adjusted_sql(
            symbols, conn=conn, cur=cur,
            table_prefix=table_prefix,
            outputsize=outputsize, sleep_time=0.1,
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


def download_and_save_daily_adjusted_sql(
        symbols, conn, cur, table_prefix='stock_quotes',
        reference_table='tickers', outputsize='full',
        sleep_time=0.1):
    metadata_table = f'{table_prefix}_metadata'
    stock_quotes_daily_table = f'{table_prefix}_daily'

    meta_query = """
                INSERT INTO {} (ticker_id, datetime, information, last_refreshed, interval, output_size, time_zone)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING metadata_id
                """.format(metadata_table)

    query = """
        INSERT INTO {} (ticker_id, date, metadata_id, open, high, low, close, adjusted_close, volume, 
        dividend_amount, split_coefficient)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker_id, date) DO NOTHING;
        """.format(stock_quotes_daily_table)

    for ticker_symbol in symbols:
        print(ticker_symbol)
        ticker_id = get_ticker_id(cur, ticker_symbol, reference_table=reference_table)

        ts = TimeSeries(key=os.environ.get('ALPHAVANTAGE_API_KEY'), output_format='pandas')
        data, meta_data = ts.get_daily_adjusted(symbol=ticker_symbol, outputsize=outputsize)

        cur.execute(
            meta_query,
            (ticker_id, pd.Timestamp.utcnow(), meta_data['1. Information'],
             meta_data['3. Last Refreshed'], '1day',
             meta_data['4. Output Size'], meta_data['5. Time Zone'])
        )
        # Fetch the returned metadata_id
        metadata_id = cur.fetchone()[0]
        conn.commit()

        # Insert data into PostgreSQL database
        for index, row in data.iterrows():
            cur.execute(
                query,
                (ticker_id, index, metadata_id, row['1. open'], row['2. high'], row['3. low'], row['4. close'],
                 row['5. adjusted close'],
                 row['6. volume'], row['7. dividend amount'], row['8. split coefficient'])
            )
        conn.commit()
        time.sleep(sleep_time)


def download_and_save_intraday_sql(
        symbols, conn, cur, table_prefix='stock_quotes', reference_table='tickers', interval='1min', outputsize='compact', month='',
        extended_hours='false',
        sleep_time=0.1):

    metadata_table = f'{table_prefix}_metadata'

    stock_quotes_intraday_table = f'{table_prefix}_{interval}'

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {stock_quotes_intraday_table} (
            id SERIAL PRIMARY KEY,
            ticker_id INT NOT NULL,
            datetime TIMESTAMP NOT NULL,
            metadata_id INT NOT NULL,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            FOREIGN KEY (ticker_id) REFERENCES {reference_table}(ticker_id),
            FOREIGN KEY (metadata_id) REFERENCES {metadata_table}(metadata_id),
            UNIQUE (ticker_id, datetime)
        );
    """)
    conn.commit()

    meta_query = """
            INSERT INTO {} (ticker_id, datetime, information, last_refreshed, interval, output_size, time_zone)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING metadata_id
            """.format(metadata_table)


    query = """
        INSERT INTO {} (ticker_id, datetime, metadata_id, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker_id, datetime) DO NOTHING;
        """.format(stock_quotes_intraday_table)

    for ticker_symbol in symbols:
        print(ticker_symbol)
        ticker_id = get_ticker_id(cur, ticker_symbol, reference_table=reference_table)

        ts = TimeSeries(key=os.environ.get('ALPHAVANTAGE_API_KEY'), output_format='pandas')
        data, meta_data = ts.get_intraday(
            symbol=ticker_symbol, interval=interval, outputsize=outputsize,
            month=month, extended_hours=extended_hours)

        cur.execute(
            meta_query,
            (ticker_id, pd.Timestamp.utcnow(), meta_data['1. Information'],
             meta_data['3. Last Refreshed'], meta_data['4. Interval'],
             meta_data['5. Output Size'], meta_data['6. Time Zone'])
        )
        # Fetch the returned metadata_id
        metadata_id = cur.fetchone()[0]
        conn.commit()
        # Insert data into PostgreSQL database
        for index, row in data.iterrows():
            cur.execute(
                query,
                (ticker_id, index, metadata_id, row['1. open'], row['2. high'], row['3. low'], row['4. close'],
                 row['5. volume'])
            )
        conn.commit()
        time.sleep(sleep_time)
