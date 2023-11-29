entry_info = {}
exit_crit = {'limit': 100, 'stop': None, 'max_hold': 10, 'signals': []}


def trade_signal(df, entry_info, exit_criteria):
    # pnl_perc = 0
    if df['high'].max() >= exit_criteria['limit']:
        pnl_perc = (exit_criteria["limit"] - entry_info['entry_price']) / entry_info['entry_price']
        print(f'sell at expected value {pnl_perc*100:.1f}%')
    else:
        pnl_perc = (df.iloc[-1]["close"] - entry_info['entry_price']) / entry_info['entry_price']
        print(f'sell at end value {pnl_perc*100:.1f}%')

    return pnl_perc
