import pandas as pd

def ranked_momentum_rotation_strategy(df, trend_window, momentum_window):
    """
    Ranked momentum rotation strategy using pandas DataFrame operations.
    
    Args:
        df: DataFrame with price columns ending in '_price'
        trend_window: Window for trend calculation (moving average)
        momentum_window: Window for momentum calculation
    
    Returns:
        Series with trading signals
    """
    signals = pd.DataFrame(index=df.index)
    
    # Calculate BTC moving average for market regime
    signals['btc_mavg'] = df['btc_price'].rolling(window=trend_window).mean()
    signals['in_market'] = df['btc_price'] > signals['btc_mavg']
    
    # Calculate momentum for all assets
    momentum_cols = []
    for col in df.filter(like='_price').columns:
        asset_name = col.split('_')[0]
        mom_col_name = f"{asset_name}_mom"
        signals[mom_col_name] = (df[col] / df[col].shift(momentum_window)) - 1
        momentum_cols.append(mom_col_name)
    
    # Find best performing asset
    signals['best_asset'] = signals[momentum_cols].idxmax(axis=1)
    
    # Generate trading signals
    signals['signal'] = 'hold_usdt'
    asset_map = {f"{col.split('_')[0]}_mom": f"hold_{col.split('_')[0]}" for col in df.filter(like='_price').columns}
    signals['best_asset_signal'] = signals['best_asset'].map(asset_map)
    signals.loc[signals['in_market'], 'signal'] = signals['best_asset_signal']
    
    return signals['signal']
