from tools.get_ticker_data import *
from tools.database_helper import *

save_type = 'psql'
outputsize = 'compact' # use 'compact' to update existing records with past 100 trading days.
reference_table='tickers'

# Connect to your postgres DB
conn = psycopg2.connect(
    dbname="stock",
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    host="localhost"
)

# Open a cursor to perform database operations
cur = conn.cursor()

# --- Intraday Stock Quotes ----

all_symbols = get_all_ticker_symbols(cur, reference_table=reference_table)
# all_symbols = ['TSLA', 'GOOG', 'IBM']
download_and_save_intraday_sql(
    all_symbols, conn, cur,
    table_prefix='stock_quotes', reference_table=reference_table, interval='5min',
    outputsize=outputsize, month='', extended_hours='false', sleep_time=1)

cur.close()
conn.close()
