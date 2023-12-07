import os
import pandas as pd


def get_ticker_id(cur, ticker_symbol, reference_table='tickers'):
    # Execute the query
    cur.execute(f"""
    SELECT ticker_id 
    FROM {reference_table}
     WHERE ticker_symbol = %s""", (ticker_symbol,))

    # Fetch the result
    ticker_id = cur.fetchone()

    if not ticker_id:
        print(f"Ticker not found for {ticker_symbol}.")
        return None

    return ticker_id[0]


def get_all_ticker_symbols(cur, reference_table='tickers'):
    # Execute the query
    cur.execute(f"""
    SELECT DISTINCT ticker_symbol 
    FROM {reference_table}""")

    # Fetch all results
    ticker_symbols = cur.fetchall()

    return [s[0] for s in ticker_symbols]


def update_reference_table(conn, cur, directory, filename_to_index, reference_table='tickers'):
    # Loop through each CSV file in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)

            # Read CSV file into DataFrame
            df = pd.read_csv(file_path)
            if filename in filename_to_index.keys():
                index_col = filename_to_index[filename]
                # Insert data from DataFrame into the database
                for index, row in df.iterrows():
                    cur.execute(
                        f"""
                            INSERT INTO {reference_table} (ticker_symbol, company, sector, industry, {index_col})
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (ticker_symbol) DO UPDATE SET
                            {index_col} = EXCLUDED.{index_col}
                        """,
                        (row.get('Ticker'), row.get('Company'), row.get('Sector'), row.get('Industry'), True)
                    )
            else:
                for index, row in df.iterrows():
                    cur.execute(
                        f"""
                            INSERT INTO {reference_table} (ticker_symbol, company, sector, industry)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (ticker_symbol) DO NOTHING
                        """,
                        (row.get('Ticker'), row.get('Company'), row.get('Sector'), row.get('Industry'))
                    )
            conn.commit()


def create_stock_database_tables(conn, cur):

    tables = {
        'reference_table': 'tickers',
        'stock_quotes_daily_table': 'stock_quotes_daily',
        'earnings_table': 'earnings',
        'dividends_table': 'dividends',
        'split_table': 'split'
    }
    reference_table = tables['reference_table']

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {reference_table} (
            ticker_id SERIAL PRIMARY KEY,
            ticker_symbol TEXT NOT NULL UNIQUE,
            company TEXT,
            sector TEXT,
            industry TEXT,
            s_and_p_500 BOOLEAN DEFAULT FALSE,
            nasdaq_100 BOOLEAN DEFAULT FALSE,
            djia BOOLEAN DEFAULT FALSE
        );
    """)
    conn.commit()

    stock_quotes_daily_table = tables['stock_quotes_daily_table']

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {stock_quotes_daily_table} (
            id SERIAL PRIMARY KEY,
            ticker_id INT NOT NULL,
            date DATE NOT NULL,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            adjusted_close FLOAT,
            volume BIGINT,
            dividend_amount FLOAT,
            split_coefficient FLOAT,
            FOREIGN KEY (ticker_id) REFERENCES {reference_table}(ticker_id),
            UNIQUE (ticker_id, date)
        );
    """)
    conn.commit()

    earnings_table = tables['earnings_table']

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {earnings_table} (
            id SERIAL PRIMARY KEY,
            ticker_id INT NOT NULL,
            date_timestamp TIMESTAMP NOT NULL,
            fiscal_period TEXT,
            fiscal_end_date BIGINT,
            eps_actual FLOAT,
            eps_estimate FLOAT,
            eps_reported_actual FLOAT,
            eps_reported_estimate FLOAT,
            sales_actual FLOAT,
            sales_estimate FLOAT,
            FOREIGN KEY (ticker_id) REFERENCES {reference_table}(ticker_id),
            UNIQUE (ticker_id, date_timestamp)
        );
    """)
    conn.commit()

    dividends_table = tables['dividends_table']

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {dividends_table} (
            id SERIAL PRIMARY KEY,
            ticker_id INT NOT NULL,
            date_timestamp TIMESTAMP NOT NULL,
            ordinary FLOAT,
            special FLOAT,
            FOREIGN KEY (ticker_id) REFERENCES {reference_table}(ticker_id),
            UNIQUE (ticker_id, date_timestamp)
        );
    """)
    conn.commit()

    split_table = tables['split_table']

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {split_table} (
            id SERIAL PRIMARY KEY,
            ticker_id INT NOT NULL,
            date_timestamp TIMESTAMP NOT NULL,
            factor_from FLOAT,
            factor_to FLOAT,
            FOREIGN KEY (ticker_id) REFERENCES {reference_table}(ticker_id),
            UNIQUE (ticker_id, date_timestamp)
        );
    """)
    conn.commit()

    return tables
