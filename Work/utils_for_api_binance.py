import requests
from pprint import pprint

import Library.utils as utils

URL = "https://api.binance.com"
URL_SPOT = "https://api.binance.com"
URL_FUTURES_USDT = "https://fapi.binance.com"  # Если в конце USDT, то без _PERP
URL_FUTURES_COIN = "https://dapi.binance.com"  # А тут с PERP на конце
#В общем, все бессрочные фьючерсы будут без PERP
AVAILABLE_INTERVALS = ("1s", "1m", "3m", "5m", "15m", "30m",
                       "1h", "2h", "4h", "6h", "8h", "12h",
                       "1d", "3d", "1w", "1M")
AVAILABLE_TRADING_PAIRS = None


def send_request_processing_params(endpoint, method, params, url_full=None):

    if url_full is None:
        url_full = URL + endpoint

    response = utils.send_request(url_full, method, params, headers={})

    return response


def get_available_trading_pairs():
    endpoint = "/api/v3/exchangeInfo"
    params = {}

    trading_pairs = dict()
    response = send_request_processing_params(endpoint, "GET", params)
    trading_pairs['SPOT'] = [
        item["symbol"] for item in response["symbols"]
    ]

    endpoint_for_coin = "/dapi/v1/exchangeInfo"
    url_full = URL_FUTURES_COIN + endpoint_for_coin
    response = send_request_processing_params(endpoint, "GET",
                                              params, url_full)
    # Вид пар ДД.ММ.ГГ
    trading_pairs['FUTURES'] = [
        item["symbol"] for item in response["symbols"]
        if "PERP" not in item["symbol"]
    ]
    trading_pairs['FUTURES_PERP'] = [
        item["symbol"][:-5] for item in response["symbols"]
        if "PERP" in item["symbol"]
    ]

    endpoint_for_usdt = "/fapi/v1/exchangeInfo"
    url_full = URL_FUTURES_USDT + endpoint_for_usdt
    response = send_request_processing_params(endpoint, "GET",
                                              params, url_full)
    trading_pairs['FUTURES'].extend([
        item["symbol"] for item in response["symbols"]
        if len(item["symbol"]) > 7 and item["symbol"][-7] == "_"
    ])
    trading_pairs['FUTURES_PERP'].extend([
        item["symbol"] for item in response["symbols"]
        if len(item["symbol"]) < 8 or item["symbol"][-7] != "_"
    ])

    return trading_pairs


def get_trading_candles(type_of_trading: str, symbol: str, interval: str,
                        start: int=None, end: int=None, limit: int=None):
    # Аккуратнее: эта функцию выдает в начале _ знзачения поздние, а потом - ранние

    # Сделай валидацию

    endpoint = "/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval
    }

    global AVAILABLE_TRADING_PAIRS
    if AVAILABLE_TRADING_PAIRS is None:
        AVAILABLE_TRADING_PAIRS = get_available_trading_pairs()

    print("\n---\n")
    print(AVAILABLE_TRADING_PAIRS['FUTURES'])
    print("\n---\n")

    if symbol not in AVAILABLE_TRADING_PAIRS[type_of_trading]:
        print("Введена невозможная торговая пара!")
        return

    if interval not in AVAILABLE_INTERVALS:
        print("Такого интервала не существует")
        return

    if start is not None:
        if start < 0 or type(start) is not int:
            print()
            return
        params["start"] = start

    if end is not None:
        if end < 0 or type(end) is not int:
            print()
            return
        params["end"] = end

    if limit is not None:
        if limit < 0 or type(limit) is not int:
            print()
            return
        params["limit"] = limit

    if type_of_trading == "FUTURES" and symbol[-7] != "_":
        print("Тип FUTURES требует даты у символа!")
        return

    if type_of_trading == "FUTURES" or type_of_trading == "FUTURES_PERP":

        # При FUTURES_PERP и FUTURES, если последние 4 буквы USDT или USDC то к url_full_usdt, иначе к другому
        if symbol[-4:] == "USDT" or symbol[-4:] == "USDC":
            url_full = URL_FUTURES_USDT + "/fapi/v1/klines"
        else:
            url_full = URL_FUTURES_COIN + "/dapi/v1/klines"

    elif type_of_trading == "SPOT":
        url_full = URL_SPOT + "/api/v3/klines"
    else:
        print("Такого типа торговли не существует")
        return

    response = send_request_processing_params(endpoint, "GET",
                                              params, url_full)

    print(url_full)

    list_of_candles = list()
    for candle in response:
        start_time = candle[0]  # In ms
        open_price = candle[1]
        high_price = candle[2]
        low_price = candle[3]
        close_price = candle[4]
        volume = candle[5]  # In basecoin, but also user use inverse contract..
        list_of_candles.append(
            (start_time, open_price, high_price,
                low_price, close_price, volume)
            )

    list_of_candles.sort(reverse=True)

    return list_of_candles


if __name__ == "__main__":

    print(get_trading_candles("SPOT", "BTCUSDT", "15m", limit=5))

