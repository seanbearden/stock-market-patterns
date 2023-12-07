
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import os
import pandas as pd
import psycopg2
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from tools.database_helper import get_ticker_id


def main(ticker_symbols,
        save_type='psql', reference_table='tickers',
        path=None, tables=None, conn=None, cur=None):
    if save_type == 'psql':
        type_to_table = {
            'chartEvent/earnings': tables['earnings_table'],
            'chartEvent/dividends': tables['dividends_table'],
            'chartEvent/split': tables['split_table']
        }

        queries = {
            'chartEvent/earnings':
                """
                INSERT INTO {} (ticker_id, date_timestamp, fiscal_period, fiscal_end_date, eps_actual, eps_estimate, 
                eps_reported_actual, eps_reported_estimate, 
                sales_actual, sales_estimate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker_id, date_timestamp) DO NOTHING;
                """.format(type_to_table['chartEvent/earnings']),
            'chartEvent/dividends':
                """
                INSERT INTO {} (ticker_id, date_timestamp, ordinary, special)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ticker_id, date_timestamp) DO NOTHING;
                """.format(type_to_table['chartEvent/dividends']),
            'chartEvent/split':
                """
                INSERT INTO {} (ticker_id, date_timestamp, factor_from, factor_to)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ticker_id, date_timestamp) DO NOTHING;
                """.format(type_to_table['chartEvent/split'])
        }



    # Replace 'your_username' and 'your_password' with your login credentials.
    USERNAME = os.environ['FINVIZ_USERNAME']
    PASSWORD = os.environ['FINVIZ_PASSWORD']

    # Replace with the actual login URL and any other URLs you need to navigate to.
    LOGIN_URL = 'https://finviz.com/login.ashx'

    # Set up Safari options, if necessary
    options = webdriver.SafariOptions()
    # options.set_preference("some_preference", "value")

    # Initialize the Safari driver
    driver = webdriver.Safari(options=options)

    # Open the login page
    driver.get(LOGIN_URL)

    # Wait for the page to load
    time.sleep(2)

    # Locate the email and password fields
    email_input = driver.find_element(By.NAME, 'email')
    password_input = driver.find_element(By.NAME, 'password')  # Replace 'password' with the actual name attribute if different

    # Enter your login credentials
    email_input.send_keys(USERNAME)
    password_input.send_keys(PASSWORD)

    # Find and click the login button
    submit_button = driver.find_element(By.XPATH, "//input[@type='submit']")
    submit_button.click()

    # Wait for the post-login page to load
    time.sleep(2)

    for ticker_symbol in ticker_symbols:
        DATA_URL = f'https://elite.finviz.com/quote.ashx?t={ticker_symbol}&p=d'

        # Navigate to the URL from which you want to scrape data
        driver.get(DATA_URL)

        # Wait for the element containing the JSON to be present
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//script[contains(text(), "var data = ")]'))
        )

        # Extract the JSON string
        json_str = element.get_attribute('innerHTML')

        # The string manipulation here is to clean the JSON string
        # by removing the variable declaration and semicolon at the end.
        json_str = json_str.split('var data = {')[1].rsplit('};\n', 1)[0]
        json_str = f'{{{json_str}}}'
        # Parse the JSON string into a Python dictionary
        data = json.loads(json_str)

        # process and save event data
        event_df = pd.DataFrame.from_dict(data['chartEvents'])
        event_df['dateTimestamp'] = pd.to_datetime(event_df['dateTimestamp'], unit='s')

        if save_type == 'hdf5':
            save_hdf5('events/' + ticker_symbol, event_df, path)
        elif save_type == 'psql':
            ticker_id = get_ticker_id(cur, ticker_symbol, reference_table=reference_table)
            if not ticker_id:
                print(f'Ticker symbol not in reference table: {ticker_symbol}')
                continue
            save_sql(conn, cur, ticker_id, queries, event_df)
        else:
            raise Exception('Unknown save type. Choose from "hdf5" or "psql"')
        time.sleep(3)
    # Quit the driver
    driver.quit()


def save_hdf5(loc, event_df, path):
    with pd.HDFStore(path, mode='a') as store:
        # Save each DataFrame to the store
        store.put(loc, event_df, format='table', data_columns=True)


def save_sql(conn, cur, ticker_id, queries, data):
    # Insert data into PostgreSQL database
    for index, row in data.iterrows():
        event_type = row['eventType']
        if event_type == 'chartEvent/earnings':
            cur.execute(
                queries[event_type],
                (ticker_id, row.get('dateTimestamp'), row.get('fiscalPeriod'), row.get('fiscalEndDate'), row.get('epsActual'),
                 row.get('epsEstimate'), row.get('epsReportedActual'), row.get('epsReportedEstimate'), row.get('salesActual'), row.get('salesEstimate'))
            )
        elif event_type == 'chartEvent/dividends':
            cur.execute(
                queries[event_type],
                (ticker_id, row.get('dateTimestamp'), row.get('ordinary'), row.get('special'))
            )
        elif event_type == 'chartEvent/split':
            cur.execute(
                queries[event_type],
                (ticker_id, row.get('dateTimestamp'), row.get('factorFrom'), row.get('factorTo'))
            )
        else:
            raise Exception(f'Unknown event type {event_type}')
    conn.commit()

