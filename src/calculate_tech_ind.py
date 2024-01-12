import os
import pandas as pd
import psycopg2
import talib
from tools.database_helper import create_stock_database_tables
from tools.pattern_helper import calculate_ichimoku, calculate_rmi
from io import StringIO


# Define the table name
stock_quotes_daily_table = 'stock_quotes_daily'
stock_quotes_daily_adj_table = 'stock_quotes_daily_adj'
stock_quotes_daily_ti_table = f'{stock_quotes_daily_adj_table}_ti'
sma_periods = [20, 50, 200]
update = False

# Database connection parameters
db_params = {
    'dbname': 'stock',
    'user': os.environ["POSTGRES_USER"],
    'password': os.environ["POSTGRES_PASSWORD"],
    'host': 'localhost',
    'port': '5432'
}

# Connect to your postgres DB
conn = psycopg2.connect(**db_params)

# Open a cursor to perform database operations
cur = conn.cursor()

# Using the with statement for managing the connection
with psycopg2.connect(**db_params) as conn:
    with conn.cursor() as cur:

        tables = create_stock_database_tables(conn, cur)

        drop_columns = ['date', 'open', 'high', 'low', 'close', 'volume']

        # Fetch distinct ticker ids
        cur.execute("""SELECT DISTINCT ticker_id FROM tickers""")
        ticker_ids = cur.fetchall()

        for ticker_id in ticker_ids:

            # Apply split coefficient to past, but not to day of split
            query = f"""
                WITH stock_date_adjusted AS (
                    SELECT
                        *, 
                        COALESCE(
                            EXP(
                                SUM(LN(split_coefficient)) OVER (
                                    ORDER BY date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                                )
                            ), 1
                        ) AS cumulative_split
                    FROM
                        {stock_quotes_daily_table}
                    WHERE 
                        ticker_id = {ticker_id[0]}
                )
                SELECT    
                    id,
                    date,
                    open / cumulative_split AS open,        
                    high / cumulative_split AS high,
                    low / cumulative_split AS low,
                    close / cumulative_split AS close,
                    volume * cumulative_split AS volume
                FROM
                    stock_date_adjusted
                WHERE 
                    ticker_id = {ticker_id[0]}
                ORDER BY
                    date;
            """

            cur.execute(query)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            df = pd.DataFrame(rows, columns=colnames)

            # Prepare the data to be inserted into PostgreSQL table
            columns = df.columns.tolist()
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)

            # Insert the DataFrame into the PostgreSQL table
            cur.copy_from(output, stock_quotes_daily_adj_table, null='', columns=columns)
            conn.commit()

            for timeperiod in sma_periods:
                df[f'sma_{timeperiod}'] = talib.SMA(df['close'], timeperiod=timeperiod)

            # Technical Indicators
            timeperiod = 14
            df[f'rsi_{timeperiod}'] = talib.RSI(df['close'], timeperiod=timeperiod)
            df[f'mfi_{timeperiod}'] = talib.MFI(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'],
                                                timeperiod=timeperiod)
            momentum_period = 5
            df[f'rmi_{timeperiod}_{momentum_period}'] = calculate_rmi(
                df['close'],
                time_period=timeperiod,
                momentum_period=momentum_period)

            fastperiod = 12
            slowperiod = 26
            signalperiod = 9

            macd = talib.MACD(df['close'], fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
            df[f'macd_{fastperiod}_{slowperiod}_{signalperiod}'] = macd[0]
            df[f'macd_signal_{fastperiod}_{slowperiod}_{signalperiod}'] = macd[1]
            df[f'macd_hist_{fastperiod}_{slowperiod}_{signalperiod}'] = macd[2]

            df = calculate_ichimoku(df, future=False)

            df.rename(columns={
                'tenkan_sen': 'ic_conversion_9_26_52',
                'kijun_sen': 'ic_base_9_26_52',
                'senkou_span_a': 'ic_span_a_9_26_52',
                'senkou_span_b': 'ic_span_b_9_26_52'
            }, inplace=True)

            df.drop(drop_columns + ['chikou_span'], axis=1, inplace=True)

            # Insert the DataFrame into the PostgreSQL table

            # which rows exist and need to be updated?
            # get ids for ticker_id that exist in ti table
            cur.execute(f"""
                SELECT ti.id 
                FROM {stock_quotes_daily_ti_table} as ti
                INNER JOIN {stock_quotes_daily_table} as s
                ON ti.id = s.id
                WHERE s.ticker_id = {ticker_id[0]}
                """)
            existing_ids = [i[0] for i in cur.fetchall()]

            idx = df.id.isin(existing_ids)
            df_existing = df[idx]
            df_new = df[~idx]

            # Define the PostgreSQL columns for the table
            columns = df.columns.tolist()

            # Bulk insert new records
            if not df_new.empty:
                # Prepare the data to be inserted into PostgreSQL table
                output = StringIO()
                df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)

                # Insert the DataFrame into the PostgreSQL table
                cur.copy_from(output, stock_quotes_daily_ti_table, null='', columns=columns)
                conn.commit()

            # Update existing records
            if update and not df_existing.empty:
                for index, row in df_existing.iterrows():
                    update_query = (f"UPDATE {stock_quotes_daily_ti_table} SET " +
                                    ", ".join([f"{col} = %s" for col in columns if col != 'id']) +
                                    " WHERE id = %s")
                    cur.execute(update_query, tuple(row[col] for col in columns if col != 'id') + (row['id'],))
                conn.commit()
