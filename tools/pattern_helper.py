import pandas as pd


def calculate_ichimoku(df, future=False, conversion_period=9, base_period=26, span_b_period=52):
    """
    Args:
        df: pandas DataFrame, input data containing high, low, and close prices
        future: boolean, indicating whether to add blank rows for future calculations (default is False)
        conversion_period: int, period for calculating Tenkan-sen (default is 9)
        base_period: int, period for calculating Kijun-sen (default is 26)
        span_b_period: int, period for calculating Senkou Span B (default is 52)

    Returns:
        df: pandas DataFrame, input data with additional columns for Ichimoku calculations

    """
    if future:
        num_rows_to_add = base_period  # number of blank rows you want to add
        empty_df = pd.DataFrame([{}] * num_rows_to_add)
        df = pd.concat([df, empty_df]).reset_index(drop=True)

    high_prices = df['high']
    close_prices = df['close']
    low_prices = df['low']
    nine_period_high = high_prices.rolling(window=conversion_period).max()
    nine_period_low = low_prices.rolling(window=conversion_period).min()
    df['tenkan_sen'] = (nine_period_high + nine_period_low) / 2
    twenty_six_period_high = high_prices.rolling(window=base_period).max()
    twenty_six_period_low = low_prices.rolling(window=base_period).min()
    df['kijun_sen'] = (twenty_six_period_high + twenty_six_period_low) / 2
    df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(base_period)
    fifty_two_period_high = high_prices.rolling(window=span_b_period).max()
    fifty_two_period_low = low_prices.rolling(window=span_b_period).min()
    df['senkou_span_b'] = ((fifty_two_period_high + fifty_two_period_low) / 2).shift(base_period)
    df['chikou_span'] = close_prices.shift(-base_period)

    return df


def convert_to_polarity(value):
    """
    Converts a numerical value into its corresponding polarity.

    Args:
        value: The numerical value to be converted.

    Returns:
        The polarity of the input value. Positive values are converted to 1, negative values are converted to -1, and zero remains unchanged.
    """
    # check if null
    if value == value:
        if value > 0:
            return 1
        elif value < 0:
            return -1
        else:
            return 0
    else:
        return value


def calculate_rmi(data, time_period=14, momentum_period=5):
    """
    Calculate the Relative Momentum Index (RMI) for a given Pandas Series.

    The definition of RMI is not well documented. This function is in alignment with RMI on Thinkorswim

    Args:
        data (pandas DataFrame): The input data.
        time_period (int, optional): The time period used for calculating the RMI. Defaults to 14.
        momentum_period (int, optional): The momentum period used for calculating the RMI. Defaults to 5.

    Returns:
        pandas DataFrame: The calculated Relative Momentum Index (RMI) values.

    """

    momentum = data.diff(periods=momentum_period)
    # Calculate U and D
    U = momentum.where(momentum > 0, 0)
    D = -momentum.where(momentum < 0, 0)

    # Calculate the EMA of U and D
    EMA_U = U.ewm(span=time_period, adjust=False).mean()
    EMA_D = D.ewm(span=time_period, adjust=False).mean()

    # Calculate the RMI
    RMI = 100 * EMA_U / (EMA_U + EMA_D)
    # remove values that are calculated from incomplete data
    RMI.iloc[:time_period + momentum_period - 1] = pd.NA

    return RMI

