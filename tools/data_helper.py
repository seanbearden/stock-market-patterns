from pandas import NA


def days_since_earnings(date, earnings_dates):
    """Function to get days since last earnings date"""
    # TECH DEBT: confirm ET in earnings dates
    # TECH DEBT: need total trading days instead of calendar days.
    past_earnings_dates = earnings_dates[earnings_dates <= date]
    if not past_earnings_dates.empty:
        last_earnings_date = past_earnings_dates.max()
        return (date - last_earnings_date).days + 1
    else:
        return NA
