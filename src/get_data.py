import get_ticker_data
import scrape_earnings_dates
path='res/data/s_and_p_study_data.h5'
get_ticker_data.main(path)  # assuming script1 has a function named main()
scrape_earnings_dates.main(path)  # assuming script2 has a function named main()
