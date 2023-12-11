import pandas as pd


def calculate_ichimoku(df, future=False):
    if future:
        num_rows_to_add = 26  # number of blank rows you want to add
        empty_df = pd.DataFrame([{}] * num_rows_to_add)
        df = pd.concat([df, empty_df]).reset_index(drop=True)

    high_prices = df['high']
    close_prices = df['close']
    low_prices = df['low']
    nine_period_high = high_prices.rolling(window=9).max()
    nine_period_low = low_prices.rolling(window=9).min()
    df['tenkan_sen'] = (nine_period_high + nine_period_low) / 2
    twenty_six_period_high = high_prices.rolling(window=26).max()
    twenty_six_period_low = low_prices.rolling(window=26).min()
    df['kijun_sen'] = (twenty_six_period_high + twenty_six_period_low) / 2
    df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(26)
    fifty_two_period_high = high_prices.rolling(window=52).max()
    fifty_two_period_low = low_prices.rolling(window=52).min()
    df['senkou_span_b'] = ((fifty_two_period_high + fifty_two_period_low) / 2).shift(26)
    df['chikou_span'] = close_prices.shift(-26)
    return df


def convert_to_polarity(value):
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

    :param data: Pandas Series of prices.
    :param time_period: The period for calculating RMI. Default is 14.
    :param momentum_period: The period for calculating momentum. Default is 5.
    :return: Pandas Series of RMI values.
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
    return RMI

