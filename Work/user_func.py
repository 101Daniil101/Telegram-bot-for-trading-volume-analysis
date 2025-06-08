import pandas as pd

import utils_for_api_bybit as bybit
import utils_for_api_okx as okx
import candle_analysis as analysis


def convert_interval(timeframe: str):
    mapping_bybit = {
            '1': '1', 
            '3': '3',
            '5': '5',
            '15': '15',
            '30': '30',
            '60': '60',
            '120': '120',
            '240': '240',
            '360': '360',
            'Day': 'D',
            'Week': 'W',
            'Month': 'M'
        }
    
    mapping_okx = {
            '1': '1m',
            '3': '3m',
            '5': '5m',
            '15': '15m',
            '30': '30m',
            '60': '1H',
            '120': '2H',
            '240': '4H',
            '360': '6H',
            'Day': '1D',
            'Week': '1W',
            'Month': '1M'
    }

    result = {"bybit": mapping_bybit[timeframe],"okx": mapping_okx[timeframe]}
    
    return result


def convert_trading_pair(trading_pair: str):
    result = {"bybit": trading_pair.replace('/', ''), "okx": trading_pair.replace('/', '-')}

    return result


def convert_type_of_trade(type_of_trade: str, trading_pair: str = ''):
    mapping_bybit = {
        'SPOT': 'spot',
        'FUTURES': 'linear',
        'PERPETUAL FUTURES': 'linear'
    }

    months_dict = {
        'JAN': '01',
        'FEB': '02',
        'MAR': '03',
        'APR': '04',
        'MAY': '05',
        'JUN': '06',
        'JUL': '07',
        'AUG': '08',
        'SEP': '09',
        'OCT': '10',
        'NOV': '11',
        'DEC': '12'
    }

    trading_pair_okx = trading_pair

    if type_of_trade == 'PERPETUAL FUTURES' and trading_pair != '':
        trading_pair_okx = trading_pair + "-SWAP"

    if type_of_trade == 'FUTURES' and trading_pair != '':
        day = trading_pair[len(trading_pair)-7:len(trading_pair_okx)-5]
        month = trading_pair[len(trading_pair)-5:len(trading_pair_okx)-2]
        year = trading_pair[len(trading_pair)-2:]

        trading_pair_okx = trading_pair[:len(trading_pair_okx)-7] + year + months_dict[month] + day

    result = {"bybit": mapping_bybit[type_of_trade], "okx": trading_pair_okx}

    return result


def fix_some_API_error(df: pd.DataFrame, type_of_trade: str):
    if (type_of_trade == "FUTURES"):
        df["volume"] = df["volume"] / 100

    if (type_of_trade == "PERPETUAL FUTURES"):
        for index in df.index:
            if df['volume', index] > 100000:
                df['volume', index] = df['volume', index] / 100
            else:
                df['volume', index] = df['volume', index] * 10

    return df


def candles_to_df(candles, exchange_name):
    df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'].astype('int64'), unit='ms')
    df = df.sort_values('timestamp')  # Сортировка по времени перед установкой индекса
    df.set_index('timestamp', inplace=True)
    df = df.astype(float)
    df['exchange'] = exchange_name
    return df


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Применяет все индикаторы к DataFrame."""
    if df.empty:
        return df

    df = analysis.calculate_obv(df)
    df = analysis.calculate_vwap(df)
    df = analysis.calculate_volume_profile(df)
    df = analysis.calculate_cmf(df)
    df = analysis.calculate_vo(df)
    df = analysis.calculate_vrsi(df)

    return df


def analys_based_on_trading_pair_timeframe_numbers_candles(trading_pair: str, type_of_trade: str, timeframe: str, numbers_of_candles: str):
    """
        Входящие параметры:
            trading_pair: str - торговая пара вида BTC/USDT. ОБЯЗАТЕЛЬНО ТАКОГО, однако если это срочные фьючерсы, то там добавляются чиселки(это тоже прошареный пользователь вводит)
            Монету срочных фьчерсов обязательно в виде: <монета>/<монета>-ДД.<первые три заглавные буквы месяца на английском языке>.ГГ 
            kind_of_trade: str - вид торговли. Принимает следующие возможные варианты: SPOT, FUTURES, PERPETUAL FUTURES
            timeframe: str - таймфрейм (сколько длится одна свеча). Принимает следующие возможные варианты (в виде строки): 
                1, 3, 5, 15, 30, 60, 120, 240, 360, Day, Week, Month. 
            numbers_of_candles: str - кол-во крайних свечей

    """
    """
        ВНИМАНИЕ! ПОКА РАБОТАЕТ ТОЛЬКО TIMEFRAME до 60 - сорян. Эээ, а хз, я пофиксил или нет...
    """

    timeframe_bybit = convert_interval(timeframe)["bybit"]
    timeframe_okx = convert_interval(timeframe)["okx"]

    trading_pair_bybit = convert_trading_pair(trading_pair)['bybit']
    trading_pair_okx = convert_trading_pair(trading_pair)['okx']

    type_of_trade_bybit = convert_type_of_trade(type_of_trade)["bybit"]
    trading_pair_okx = convert_type_of_trade(
        type_of_trade, trading_pair_okx
        )["okx"]

    list_of_candles_bybit = bybit.get_trading_candles(
        type_of_trade_bybit, trading_pair_bybit,
        timeframe_bybit, limit=int(numbers_of_candles)
        )

    list_of_candles_okx = okx.get_trading_candles(
        trading_pair_okx, timeframe_okx,
        limit=numbers_of_candles
    )

    df_bybit = candles_to_df(list_of_candles_bybit, 'Bybit')
    df_okx = candles_to_df(list_of_candles_okx, 'OKX')

    df_okx = fix_some_API_error(df_okx, type_of_trade)

    df_bybit = analysis.calculate_obv(df_bybit)
    df_okx = analysis.calculate_obv(df_okx)

    df_bybit = analysis.calculate_vwap(df_bybit)
    df_okx = analysis.calculate_vwap(df_okx)

    # df_bybit = analysis.calculate_volume_profile(df_bybit)
    # df_okx = analysis.calculate_volume_profile(df_okx)

    df_bybit = analysis.calculate_cmf(df_bybit)
    df_okx = analysis.calculate_cmf(df_okx)

    # df_bybit = analysis.calculate_vo(df_bybit)
    # df_okx = analysis.calculate_vo(df_okx)

    # df_bybit = analysis.calculate_vrsi(df_bybit)
    # df_okx = analysis.calculate_vrsi(df_okx)

    exchange_comparison = analysis.compare_exchanges(df_bybit, df_okx) 

    df_combined = pd.concat([df_bybit, df_okx])
    df_combined.sort_index(inplace=True)

    print(df_bybit)
    print(df_okx)

    print(exchange_comparison)  # правильно отработало на таймфрейме: 1, 3, 5, 15, 30, 60, 120, 240 - это все из-за разницы в представлении времени бирж


analys_based_on_trading_pair_timeframe_numbers_candles("BTC/USDT-06JUN25", "FUTURES", "240", '4')
print("\n---\n")
analys_based_on_trading_pair_timeframe_numbers_candles("BTC/USDT-06JUN25", "FUTURES", "360", '4')

# Добавь обработку ошибок

# analys_based_on_trading_pair_timeframe_numbers_candles("BTC/USDT", "PERPETUAL FUTURES", "3", '3') на этой паре правильно отработало на таймфрейме: 1, 3, 5, 15, 30, 60, 120, 240
# Но нужно умножить на 10 PERPETUAL FUTURES
# Если больше 1k то нужно разделить на 100 PERPETUAL FUTURES
# Нужно разделить на 100 FUTURES
# Неправильный объем выдает


# В общем, все работает, просто нужно внимательно смотреть за временем. Выводится все считая, что UTC +0Выбранная торговая пара
