
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import time


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

sandp500_df = pd.read_csv('res/indices/s_and_p_500_details.csv')

for symbol in sandp500_df['Ticker']:
    print(symbol)
    DATA_URL = f'https://elite.finviz.com/quote.ashx?t={symbol}&p=d'

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

    with pd.HDFStore('res/data/s_and_p_events.h5', mode='a') as store:
        # Save each DataFrame to the store
        store.put(symbol, event_df, format='table', data_columns=True)

    time.sleep(3)
# Quit the driver
driver.quit()

