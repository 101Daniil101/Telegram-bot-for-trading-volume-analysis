import pandas as pd
from datetime import datetime

import utils_for_api_bybit as bybit
import utils_for_api_okx as okx
import utils_for_api_binance as binance
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

    mapping_binance = {
            '1': '1m',
            '3': '3m',
            '5': '5m',
            '15': '15m',
            '30': '30m',
            '60': '1h',
            '120': '2h',
            '240': '4h',
            '360': '6h',
            'Day': '1d',
            'Week': '1w',
            'Month': '1M'
    }

    result = {"bybit": mapping_bybit[timeframe], "okx": mapping_okx[timeframe],
              "binance": mapping_binance[timeframe]}

    return result


def convert_trading_pair(trading_pair: str, exchange: str, type_of_trade: str):
    result = {"bybit": trading_pair.replace('/', ''), "okx": trading_pair.replace('/', '-'),
              "binance": trading_pair.replace('/', '')}

    trading_pair = result[exchange]

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

    if type_of_trade == "FUTURES":
        day = trading_pair[len(trading_pair)-7:len(trading_pair)-5]
        month = trading_pair[len(trading_pair)-5:len(trading_pair)-2]
        year = trading_pair[len(trading_pair)-2:]
        print(1)

        if exchange == 'binance':
            print(2)
            trading_pair = trading_pair[:len(trading_pair)-7] + year + months_dict[month] + day

            trading_pair = trading_pair.replace('-', '_')
            print(trading_pair)
            result['binance'] = trading_pair
        elif exchange == 'okx':
            trading_pair = trading_pair[:len(trading_pair)-7] + year + months_dict[month] + day
            result['okx'] = trading_pair

    return result


def convert_type_of_trade(type_of_trade: str, trading_pair: str = '',
                          exchange: str = ''):

    result = dict()

    mapping_bybit = {
        'SPOT': 'spot',
        'FUTURES': 'linear',
        'PERPETUAL FUTURES': 'linear'
    }

    mapping_binance = {
        'SPOT': 'SPOT',
        'FUTURES': 'FUTURES',
        'PERPETUAL FUTURES': 'FUTURES_PERP'
    }

    result['binance'] = mapping_binance[type_of_trade]

    trading_pair_okx = trading_pair

    if type_of_trade == 'PERPETUAL FUTURES' and trading_pair != '' and exchange == 'okx':  # Это тоже посути в convert_trading_pair
        trading_pair_okx = trading_pair + "-SWAP"

    result['bybit'] = mapping_bybit[type_of_trade]
    result['okx'] = trading_pair_okx

    return result


def fix_some_API_error(df: pd.DataFrame, type_of_trade: str):
    if (type_of_trade == "FUTURES"):
        df["volume"] = df["volume"] / 100

    if (type_of_trade == "PERPETUAL FUTURES"):
        for index in df.index:
            if df.loc[index, 'volume'] > 100000:
                df.loc[index, 'volume'] = df.loc[index, 'volume'] / 100
            else:
                df.loc[index, 'volume'] = df.loc[index, 'volume'] * 10

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


def readable_time_to_ms(time_str) -> int:
    """
    Преобразует строку вида 'ДД.ММ.ГГГГ ЧЧ:ММ' в миллисекунды с момента Unix-эпохи.
    
    :param time_str: Строка времени в формате 'ДД.ММ.ГГГГ ЧЧ:ММ'
    :return: Целое число миллисекунд или выбрасывает ValueError при неверном формате
    """
    # Парсим строку как datetime объект
    dt = datetime.strptime(time_str, "%d.%m.%Y %H:%M")
    
    # Вычисляем количество миллисекунд
    milliseconds = int(dt.timestamp() * 1000)
    
    return milliseconds


# На бирже Binance во FUTURES в начале стоит год, а в конце дата

def analys_based_on_trading_pair_timeframe_numbers_candles(trading_pair: str, type_of_trade: str, timeframe: str, numbers_of_candles: str):
    """
        Входящие параметры:
            trading_pair: str - торговая пара вида BTC/USDT. ОБЯЗАТЕЛЬНО ТАКОГО
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
    timeframe_binance = convert_interval(timeframe)["binance"]

    trading_pair_bybit = convert_trading_pair(trading_pair, 'bybit', type_of_trade)['bybit']
    trading_pair_okx = convert_trading_pair(trading_pair, 'okx', type_of_trade)['okx']
    trading_pair_binance = convert_trading_pair(trading_pair, 'binance', type_of_trade)['binance']

    type_of_trade_bybit = convert_type_of_trade(type_of_trade)["bybit"]
    trading_pair_okx = convert_type_of_trade(
        type_of_trade, trading_pair_okx
        )["okx"]
    type_of_trade_binance = convert_type_of_trade(type_of_trade)["binance"]

    list_of_candles_bybit = bybit.get_trading_candles(
        type_of_trade_bybit, trading_pair_bybit,
        timeframe_bybit, limit=int(numbers_of_candles)
        )

    list_of_candles_okx = okx.get_trading_candles(
        trading_pair_okx, timeframe_okx,
        limit=numbers_of_candles
    )

    list_of_candles_binance = binance.get_trading_candles(
        type_of_trade_binance, trading_pair_binance, timeframe_binance,
        limit=int(numbers_of_candles)
    )

    df_bybit = candles_to_df(list_of_candles_bybit, 'Bybit')
    df_okx = candles_to_df(list_of_candles_okx, 'OKX')
    df_binance = candles_to_df(list_of_candles_binance, 'Binance')

    df_okx = fix_some_API_error(df_okx, type_of_trade)

    df_bybit = analysis.calculate_obv(df_bybit)
    df_okx = analysis.calculate_obv(df_okx)
    df_binance = analysis.calculate_obv(df_binance)

    df_bybit = analysis.calculate_vwap(df_bybit)
    df_okx = analysis.calculate_vwap(df_okx)
    df_binance = analysis.calculate_vwap(df_binance)

    # df_bybit = analysis.calculate_volume_profile(df_bybit)
    # df_okx = analysis.calculate_volume_profile(df_okx)

    df_bybit = analysis.calculate_cmf(df_bybit)
    df_okx = analysis.calculate_cmf(df_okx)
    df_binance = analysis.calculate_cmf(df_binance)

    # df_bybit = analysis.calculate_vo(df_bybit)
    # df_okx = analysis.calculate_vo(df_okx)

    # df_bybit = analysis.calculate_vrsi(df_bybit)
    # df_okx = analysis.calculate_vrsi(df_okx)

    # exchange_comparison = analysis.compare_exchanges(df_bybit, df_okx)  Добавь сюда обработку с бинансом

    df_combined = pd.concat([df_bybit, df_okx, df_binance])
    df_combined.sort_index(inplace=True)

    # РАЗБИРАЙСЯ С PERPETUAL FUTURES

    print(df_bybit)
    print(df_okx)
    print(df_binance)
    print(df_combined)

    # print(exchange_comparison)  # правильно отработало на таймфрейме: 1, 3, 5, 15, 30, 60, 120, 240 - это все из-за разницы в представлении времени бирж


def analys_based_on_trading_pair_timeframe_start_end(trading_pair: str, type_of_trade: str, timeframe: str, start_time: str, end_time: str):
    """
        Входящие параметры:
            trading_pair: str - торговая пара вида BTC/USDT. ОБЯЗАТЕЛЬНО ТАКОГО
            Монету срочных фьчерсов обязательно в виде: <монета>/<монета>-ДД.<первые три заглавные буквы месяца на английском языке>.ГГ 
            type_of_trade: str - вид торговли. Принимает следующие возможные варианты: SPOT, FUTURES, PERPETUAL FUTURES
            timeframe: str - таймфрейм (сколько длится одна свеча). Принимает следующие возможные варианты (в виде строки): 
                1, 3, 5, 15, 30, 60, 120, 240, 360, Day, Week, Month. 
            start_time: str - время открытия первой свечи в формате ИМЕННО ТАКОМ ДД.ММ.ГГГГ ЧЧ:ММ (при выборе лучше сделать через календарь конечно)
            end_time: str - время закрытия последней свечи в формате ИМЕННО ТАКОМ ДД.ММ.ГГГГ ЧЧ:ММ (при выборе лучше сделать через календарь конечно)
            То есть в результат пойдут свечи, время открытия которых больше start_time, но меньше end_time
    """
    """
        ВНИМАНИЕ! Функция сравнения токенов на биржах будет работать только с timeframe до 60 (это связано с тем, в какое время начинает отчет и бирж)
    """

    timeframe_bybit = convert_interval(timeframe)["bybit"]
    timeframe_okx = convert_interval(timeframe)["okx"]
    timeframe_binance = convert_interval(timeframe)["binance"]

    trading_pair_bybit = convert_trading_pair(trading_pair, 'bybit', type_of_trade)['bybit']
    trading_pair_okx = convert_trading_pair(trading_pair, 'okx', type_of_trade)['okx']
    trading_pair_binance = convert_trading_pair(trading_pair, 'binance', type_of_trade)['binance']

    type_of_trade_bybit = convert_type_of_trade(type_of_trade)["bybit"]
    trading_pair_okx = convert_type_of_trade(
        type_of_trade, trading_pair_okx
        )["okx"]
    type_of_trade_binance = convert_type_of_trade(type_of_trade)["binance"]

    # after = end
    start_time = readable_time_to_ms(start_time)
    end_time = readable_time_to_ms(end_time)

    list_of_candles_bybit = bybit.get_trading_candles(
        type_of_trade_bybit, trading_pair_bybit,
        timeframe_bybit, start=start_time, end=end_time
        )

    list_of_candles_okx = okx.get_trading_candles(
        trading_pair_okx, timeframe_okx,
        after=str(end_time+1), before=str(start_time-1)  # Потому что OKX не включает диапазоны
    )

    list_of_candles_binance = binance.get_trading_candles(
        type_of_trade_binance, trading_pair_binance, timeframe_binance,
        start=start_time, end=end_time
    )

    df_bybit = candles_to_df(list_of_candles_bybit, 'Bybit')
    df_okx = candles_to_df(list_of_candles_okx, 'OKX')
    df_binance = candles_to_df(list_of_candles_binance, 'Binance')

    df_okx = fix_some_API_error(df_okx, type_of_trade)

    df_bybit = analysis.calculate_obv(df_bybit)
    df_okx = analysis.calculate_obv(df_okx)
    df_binance = analysis.calculate_obv(df_binance)

    df_bybit = analysis.calculate_vwap(df_bybit)
    df_okx = analysis.calculate_vwap(df_okx)
    df_binance = analysis.calculate_vwap(df_binance)

    df_bybit = analysis.calculate_cmf(df_bybit)
    df_okx = analysis.calculate_cmf(df_okx)
    df_binance = analysis.calculate_cmf(df_binance)

    df_combined = pd.concat([df_bybit, df_okx, df_binance])
    df_combined.sort_index(inplace=True)

    print(df_bybit)
    print(df_okx)
    print(df_binance)
    print(df_combined)


if __name__ == "__main__":
    # analys_based_on_trading_pair_timeframe_numbers_candles("BTC/USDT-26SEP25", "FUTURES", "15", '4')
    analys_based_on_trading_pair_timeframe_start_end("BTC/USDT", "SPOT", "15", "12.06.2025 09:00", "12.06.2025 09:30")

#Bybit включает в диапзаон
#OKX не включает в дапазаон
#Binance все включает

# Добавь обработку ошибок

# analys_based_on_trading_pair_timeframe_numbers_candles("BTC/USDT", "PERPETUAL FUTURES", "3", '3') на этой паре правильно отработало на таймфрейме: 1, 3, 5, 15, 30, 60, 120, 240
# Но нужно умножить на 10 PERPETUAL FUTURES
# Если больше 1k то нужно разделить на 100 PERPETUAL FUTURES
# Нужно разделить на 100 FUTURES
# Неправильный объем выдает


# В общем, все работает, просто нужно внимательно смотреть за временем. Выводится все считая, что UTC +0Выбранная торговая пара
