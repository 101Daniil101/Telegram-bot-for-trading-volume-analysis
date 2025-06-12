import numpy as np
import pandas as pd

import utils_for_api_bybit as bybit
import utils_for_api_okx as okx


def calculate_obv(df):
    df['obv'] = (np.sign(df['close'].diff()) * df['volume']).cumsum()
    return df


def calculate_vwap(df):
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    df['vwap'] = (df['typical_price'] * df['volume']).cumsum() / \
        df['volume'].cumsum()
    return df


def calculate_volume_profile(df, price_bins=20):
    df['price_bin'] = pd.cut(df['close'], bins=price_bins)
    volume_profile = df.groupby('price_bin', observed=False)['volume'] \
        .sum().sort_values(ascending=False)
    return volume_profile
