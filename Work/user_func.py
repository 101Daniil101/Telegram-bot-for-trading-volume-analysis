import pandas as pd
from datetime import datetime

import utils_for_api_bybit as bybit
import utils_for_api_okx as okx
import utils_for_api_binance as binance
import candle_analysis as analysis
import create_graphs as graphs


def convert_interval(timeframe: str):
    mapping_bybit = {
        '1': '1', '3': '3', '5': '5', '15': '15', '30': '30',
        '60': '60', '120': '120', '240': '240', '360': '360',
        'Day': 'D', 'Week': 'W', 'Month': 'M'
    }

    mapping_okx = {
        '1': '1m', '3': '3m', '5': '5m', '15': '15m', '30': '30m',
        '60': '1H', '120': '2H', '240': '4H', '360': '6H',
        'Day': '1D', 'Week': '1W', 'Month': '1M'
    }

    mapping_binance = {
        '1': '1m', '3': '3m', '5': '5m', '15': '15m', '30': '30m',
        '60': '1h', '120': '2h', '240': '4h', '360': '6h',
        'Day': '1d', 'Week': '1w', 'Month': '1M'
    }
    result = {"bybit": mapping_bybit[timeframe], "okx": mapping_okx[timeframe],
              "binance": mapping_binance[timeframe]}

    return result


def convert_trading_pair(trading_pair: str, exchange: str, type_of_trade: str):
    result = {"bybit": trading_pair.replace('/', ''),
              "okx": trading_pair.replace('/', '-'),
              "binance": trading_pair.replace('/', '')}

    trading_pair = result[exchange]

    months_dict = {
        'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
        'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
        'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
    }

    if type_of_trade == "FUTURES":
        day = trading_pair[len(trading_pair)-7:len(trading_pair)-5]
        month = trading_pair[len(trading_pair)-5:len(trading_pair)-2]
        year = trading_pair[len(trading_pair)-2:]

        if exchange == 'binance':
            trading_pair = trading_pair[:len(trading_pair)-7] + year \
                           + months_dict[month] + day

            trading_pair = trading_pair.replace('-', '_')
            result['binance'] = trading_pair
        elif exchange == 'okx':
            trading_pair = trading_pair[:len(trading_pair)-7] + year \
                           + months_dict[month] + day
            result['okx'] = trading_pair

    elif type_of_trade == "PERPETUAL FUTURES" and exchange == "okx":
        trading_pair = trading_pair + "-SWAP"
        result['okx'] = trading_pair

    return result


def convert_type_of_trade(type_of_trade: str, trading_pair: str = '',
                          exchange: str = ''):
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

    result = {
        'binance': mapping_binance[type_of_trade],
        'bybit': mapping_bybit[type_of_trade]
        }

    return result


def fix_some_API_error(df: pd.DataFrame, type_of_trade: str):
    if (type_of_trade == "FUTURES"):
        df["volume"] = df["volume"] / 100

    if (type_of_trade == "PERPETUAL FUTURES"):
        for index in df.index:
            if df.loc[index, 'volume'] > 10000:
                df.loc[index, 'volume'] = df.loc[index, 'volume'] / 100
            else:
                df.loc[index, 'volume'] = df.loc[index, 'volume'] * 10

    return df


def candles_to_df(candles, exchange_name):
    df = pd.DataFrame(
        candles,
        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )
    df['timestamp'] = pd.to_datetime(
        df['timestamp'].astype('int64'), unit='ms'
        )
    df = df.sort_values('timestamp')
    df.set_index('timestamp', inplace=True)
    df = df.astype(float)
    df['exchange'] = exchange_name
    return df


def readable_time_to_ms(time_str) -> int:
    dt = datetime.strptime(time_str, "%d.%m.%Y %H:%M")
    milliseconds = int(dt.timestamp() * 1000)

    return milliseconds


def analys_based_on_trading_pair_timeframe_numbers_candles(
        trading_pair: str, type_of_trade: str,
        timeframe: str, numbers_of_candles: str
        ):
    """
        Входящие параметры:
            trading_pair: str - торговая пара вида BTC/USDT. ОБЯЗАТЕЛЬНО ТАКОГО
            Монету срочных фьчерсов обязательно в виде:
            <монета>/<монета>-ДД.<первые три заглавные буквы месяца на английском языке>.ГГ
            
            kind_of_trade: str - вид торговли.
            Принимает следующие возможные варианты:
                SPOT, FUTURES, PERPETUAL FUTURES
            
            timeframe: str - таймфрейм (сколько длится одна свеча).
            Принимает следующие возможные варианты (в виде строки):
                1, 3, 5, 15, 30, 60, 120, 240, 360, Day, Week, Month.

            numbers_of_candles: str - кол-во крайних свечей

    """

    timeframe_bybit = convert_interval(timeframe)["bybit"]
    timeframe_okx = convert_interval(timeframe)["okx"]
    timeframe_binance = convert_interval(timeframe)["binance"]

    trading_pair_bybit = convert_trading_pair(
        trading_pair, 'bybit', type_of_trade
        )['bybit']
    trading_pair_okx = convert_trading_pair(
        trading_pair, 'okx', type_of_trade
        )['okx']
    trading_pair_binance = convert_trading_pair(
        trading_pair, 'binance', type_of_trade
        )['binance']

    type_of_trade_bybit = convert_type_of_trade(type_of_trade)["bybit"]
    type_of_trade_binance = convert_type_of_trade(type_of_trade)["binance"]

    if ((list_of_candles_bybit := bybit.get_trading_candles(
        type_of_trade_bybit, trading_pair_bybit,
        timeframe_bybit, limit=int(numbers_of_candles)
        )) is None):
        # Сюда добавь аварийный выход и вывод ошибки в тг бота:
        # "Ошибка валидации данных, проверьте вводимые данные.
        # Для подробнестей обратитесь к админу"
        ...

    if ((list_of_candles_okx := okx.get_trading_candles(
        trading_pair_okx, timeframe_okx,
        limit=numbers_of_candles
        )) is None):
        # Сюда добавь аварийный выход и вывод ошибки в тг бота:
        # "Ошибка валидации данных, проверьте вводимые данные.
        # Для подробнестей обратитесь к админу"
        ...

    if ((list_of_candles_binance := binance.get_trading_candles(
        type_of_trade_binance, trading_pair_binance, timeframe_binance,
        limit=int(numbers_of_candles)
        )) is None):
        # Сюда добавь аварийный выход и вывод ошибки в тг бота:
        # "Ошибка валидации данных, проверьте вводимые данные.
        # Для подробнестей обратитесь к админу"
        ...

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

    df_volume_profile_bybit = analysis.calculate_volume_profile(df_bybit)
    df_volume_profile_okx = analysis.calculate_volume_profile(df_okx)
    df_volume_profile_binance = analysis.calculate_volume_profile(df_binance)

    path_to_volume_plot = graphs.create_volume_plot(
        df_bybit, df_okx, df_binance
        )
    path_to_obv_plot = graphs.create_obv_plot(
        df_bybit, df_okx, df_binance
        )
    path_to_plot_vwap = graphs.create_plot_vwap(
        df_bybit, df_okx, df_binance
        )
    path_to_volume_pie_chart = graphs.create_volume_pie_chart(
        df_bybit, df_okx, df_binance
        )
    path_to_plot_volume_profilies = graphs.create_plot_volume_profiles(
        df_volume_profile_bybit, df_volume_profile_okx,
        df_volume_profile_binance
        )

    return (
            path_to_volume_plot, path_to_obv_plot, path_to_plot_vwap,
            path_to_volume_pie_chart, path_to_plot_volume_profilies
            )


def analys_based_on_trading_pair_timeframe_start_end(
        trading_pair: str, type_of_trade: str, timeframe: str,
        start_time: str, end_time: str
        ):
    """
        Входящие параметры:
            trading_pair: str - торговая пара вида BTC/USDT. ОБЯЗАТЕЛЬНО ТАКОГО
            Монету срочных фьчерсов обязательно в виде:
            <монета>/<монета>-ДД.<первые три заглавные буквы месяца на английском языке>.ГГ
            
            kind_of_trade: str - вид торговли.
            Принимает следующие возможные варианты:
                SPOT, FUTURES, PERPETUAL FUTURES
            
            timeframe: str - таймфрейм (сколько длится одна свеча).
            Принимает следующие возможные варианты (в виде строки):
                1, 3, 5, 15, 30, 60, 120, 240, 360, Day, Week, Month.

            start_time: str - время открытия первой свечи в формате
            ИМЕННО ТАКОМ ДД.ММ.ГГГГ ЧЧ:ММ
            (при выборе лучше сделать через календарь конечно)

            end_time: str - время закрытия последней свечи в формате
            ИМЕННО ТАКОМ ДД.ММ.ГГГГ ЧЧ:ММ
            (при выборе лучше сделать через календарь конечно)

            То есть в результат пойдут свечи, время открытия которых больше 
            или равны start_time, но меньше или равны end_time
    """

    timeframe_bybit = convert_interval(timeframe)["bybit"]
    timeframe_okx = convert_interval(timeframe)["okx"]
    timeframe_binance = convert_interval(timeframe)["binance"]

    trading_pair_bybit = convert_trading_pair(
        trading_pair, 'bybit', type_of_trade
        )['bybit']
    trading_pair_okx = convert_trading_pair(
        trading_pair, 'okx', type_of_trade
        )['okx']
    trading_pair_binance = convert_trading_pair(
        trading_pair, 'binance', type_of_trade
        )['binance']

    type_of_trade_bybit = convert_type_of_trade(type_of_trade)["bybit"]
    type_of_trade_binance = convert_type_of_trade(type_of_trade)["binance"]

    start = readable_time_to_ms(start_time)
    end = readable_time_to_ms(end_time)

    if ((list_of_candles_bybit := bybit.get_trading_candles(
        type_of_trade_bybit, trading_pair_bybit,
        timeframe_bybit, start=start, end=end
        )) is None):
        # Сюда добавь аварийный выход и вывод ошибки в тг бота:
        # "Ошибка валидации данных, проверьте вводимые данные.
        # Для подробнестей обратитесь к админу"
        ...

    if ((list_of_candles_okx := okx.get_trading_candles(
        trading_pair_okx, timeframe_okx,
        after=str(end+1), before=str(start-1)
        )) is None):
        # Сюда добавь аварийный выход и вывод ошибки в тг бота:
        # "Ошибка валидации данных, проверьте вводимые данные.
        # Для подробнестей обратитесь к админу"
        ...

    if ((list_of_candles_binance := binance.get_trading_candles(
        type_of_trade_binance, trading_pair_binance, timeframe_binance,
        start=start, end=end
        )) is None):
        # Сюда добавь аварийный выход и вывод ошибки в тг бота:
        # "Ошибка валидации данных, проверьте вводимые данные.
        # Для подробнестей обратитесь к админу"
        ...

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

    df_volume_profile_bybit = analysis.calculate_volume_profile(df_bybit)
    df_volume_profile_okx = analysis.calculate_volume_profile(df_okx)
    df_volume_profile_binance = analysis.calculate_volume_profile(df_binance)

    path_to_volume_plot = graphs.create_volume_plot(
        df_bybit, df_okx, df_binance
        )
    path_to_obv_plot = graphs.create_obv_plot(
        df_bybit, df_okx, df_binance
        )
    path_to_plot_vwap = graphs.create_plot_vwap(
        df_bybit, df_okx, df_binance
        )
    path_to_volume_pie_chart = graphs.create_volume_pie_chart(
        df_bybit, df_okx, df_binance
        )
    path_to_plot_volume_profilies = graphs.create_plot_volume_profiles(
        df_volume_profile_bybit, df_volume_profile_okx,
        df_volume_profile_binance
        )

    return (
            path_to_volume_plot, path_to_obv_plot, path_to_plot_vwap,
            path_to_volume_pie_chart, path_to_plot_volume_profilies
            )


if __name__ == "__main__":
    print(analys_based_on_trading_pair_timeframe_numbers_candles(
        "BTC/USDT-26SEP25", "FUTURES", "15", '4'
        ))
    print(analys_based_on_trading_pair_timeframe_start_end(
        "BTC/USDT", "SPOT", "15", "12.06.2025 09:00", "12.06.2025 09:30"
        ))
    print(analys_based_on_trading_pair_timeframe_numbers_candles(
        "BTC/USDT", "PERPETUAL FUTURES", "60", '5'
        ))
