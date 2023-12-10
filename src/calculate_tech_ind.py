import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import talib
from tools.database_helper import get_all_ticker_symbols
from tools.pattern_helper import calculate_ichimoku, calculate_rmi

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

ticker_id = 1

drop_columns = ['high', 'low', 'close', 'volume']

cur.execute("""SELECT DISTINCT ticker_id FROM tickers""")
ticker_ids = cur.fetchall()

# SQLAlchemy engine for PostgreSQL
engine = create_engine(
    f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

for ticker_id in ticker_ids:
    query = f"""
        SELECT id, {','.join(drop_columns)}
        FROM stock_quotes_daily
        WHERE ticker_id = {ticker_id[0]}
        ORDER BY date ASC;
        """

    cur.execute(query)
    rows = cur.fetchall()
    # Get the column names
    colnames = [desc[0] for desc in cur.description]



    # Create a DataFrame from the rows and column names
    df = pd.DataFrame(rows, columns=colnames)

    for timeperiod in [20, 50, 200]:
        df[f'sma_{timeperiod}'] = talib.SMA(df['close'], timeperiod=timeperiod)


    # Technical Indicators
    timeperiod = 14
    df[f'rsi_{timeperiod}'] = talib.RSI(df['close'], timeperiod=timeperiod)
    df[f'mfi_{timeperiod}'] = talib.MFI(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'], timeperiod=timeperiod)
    momentum_period = 5
    df[f'rmi_{timeperiod}_{momentum_period}'] = calculate_rmi(df['close'], time_period=timeperiod, momentum_period=momentum_period)

    fastperiod=12
    slowperiod=26
    signalperiod=9

    macd = talib.MACD(df['close'], fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
    df[f'macd_{fastperiod}_{slowperiod}_{signalperiod}'] = macd[0]
    df[f'macd_signal_{fastperiod}_{slowperiod}_{signalperiod}'] = macd[1]
    df[f'macd_hist_{fastperiod}_{slowperiod}_{signalperiod}'] = macd[2]

    df = calculate_ichimoku(df)

    df.rename(columns={
        'tenkan_sen': 'ic_conversion_9_26_52',
        'kijun_sen': 'ic_base_9_26_52',
        'senkou_span_a': 'ic_span_a_9_26_52',
        'senkou_span_b': 'ic_span_b_9_26_52'
    }, inplace=True)

    df.drop(drop_columns + ['chikou_span'], axis=1, inplace=True)

    # Define the table name
    stock_quotes_daily_ti_table = 'stock_quotes_daily_ti'

    # Insert the DataFrame into the PostgreSQL table
    df.to_sql(stock_quotes_daily_ti_table, engine, if_exists='replace', index=False, method='multi')

# Close the engine connection
engine.dispose()
cur.close()
conn.close()