def ranked_momentum_rotation_strategy(df, trend_window, momentum_window):
    signals = pd.DataFrame(index=df.index)
    signals['btc_mavg'] = df['btc_price'].rolling(window=trend_window).mean()
    signals['in_market'] = df['btc_price'] > signals['btc_mavg']
    momentum_cols = []
    for col in df.filter(like='_price').columns:
        asset_name = col.split('_')[0]; mom_col_name = f"{asset_name}_mom"
        signals[mom_col_name] = (df[col] / df[col].shift(momentum_window)) - 1
        momentum_cols.append(mom_col_name)
    
    signals['best_asset'] = signals[momentum_cols].idxmax(axis=1)
    
    signals['signal'] = 'hold_usdt'
    asset_map = {f"{col.split('_')[0]}_mom": f"hold_{col.split('_')[0]}" for col in df.filter(like='_price').columns}
    signals['best_asset_signal'] = signals['best_asset'].map(asset_map)
    signals.loc[signals['in_market'], 'signal'] = signals['best_asset_signal']
    return signals['signal']
