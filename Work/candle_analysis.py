import numpy as np
import pandas as pd

import utils_for_api_bybit as bybit
import utils_for_api_okx as okx


def calculate_obv(df):
    df['obv'] = (np.sign(df['close'].diff()) * df['volume']).cumsum()
    return df


def calculate_vwap(df):
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    df['vwap'] = (df['typical_price'] * df['volume']).cumsum() / df['volume'].cumsum()
    return df


def calculate_volume_profile(df, price_bins=20):
    df['price_bin'] = pd.cut(df['close'], bins=price_bins)
    volume_profile = df.groupby('price_bin', observed=False)['volume'].sum().sort_values(ascending=False)
    return volume_profile


def calculate_cmf(df, window=20):
    clv = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
    clv = clv.fillna(0)
    df['cmf'] = (clv * df['volume']).rolling(window).sum() / df['volume'].rolling(window).sum()
    return df


def calculate_vrsi(df, window=14):
    delta = df['volume'].diff(1)
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()
    rs = avg_gain / avg_loss
    df['vrsi'] = 100 - (100 / (1 + rs))
    return df


def calculate_vo(df, short_window=5, long_window=20):
    ma_short = df['volume'].rolling(short_window).mean()
    ma_long = df['volume'].rolling(long_window).mean()
    df['vo'] = ((ma_short - ma_long) / ma_long) * 100
    return df


def compare_exchanges(df1, df2):
    comparison = pd.DataFrame()
    comparison['price_diff'] = df1['close'] - df2['close']
    comparison['volume_diff'] = df1['volume'] - df2['volume']
    comparison['price_ratio'] = df1['close'] / df2['close']
    comparison['volume_ratio'] = df1['volume'] / df2['volume']
    return comparison
