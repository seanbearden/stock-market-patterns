import psycopg2
from tools import get_ticker_data, scrape_events
from tools.database_helper import *

save_type = 'psql'
outputsize = 'compact' # use 'compact' to update existing records with past 100 trading days.

# Database connection parameters
db_params = {
    'dbname': 'stock',
    'user': os.environ["POSTGRES_USER"],
    'password': os.environ["POSTGRES_PASSWORD"],
    'host': 'localhost',
}

# Connect to your postgres DB
conn = psycopg2.connect(**db_params)

# Open a cursor to perform database operations
cur = conn.cursor()

tables = create_stock_database_tables(conn, cur)

# --- Reference Table for Ticker Symbols ----

filename_to_index = {
    'nasdaq_100_details.csv': 'nasdaq_100',
    'djia_details.csv': 'djia',
    's_and_p_500_details.csv': 's_and_p_500',
}
# Directory containing CSV files
directory = 'res/indices'

update_reference_table(conn, cur, directory, filename_to_index, reference_table=tables['reference_table'])

# --- Daily Stock Quotes ----

all_symbols = get_all_ticker_symbols(cur, reference_table=tables['reference_table'])

# download and save ticker data
get_ticker_data.main(
    all_symbols,
    save_type=save_type,
    outputsize=outputsize,
    conn=conn,
    cur=cur
)

# --- Chart Events ----

type_to_table = {
    'chartEvent/earnings': tables['earnings_table'],
    'chartEvent/dividends': tables['dividends_table'],
    'chartEvent/split': tables['split_table']
}

scrape_events.main(
    ticker_symbols=all_symbols,
    tables=tables,
    save_type=save_type,
    conn=conn,
    cur=cur
)

cur.close()
conn.close()
