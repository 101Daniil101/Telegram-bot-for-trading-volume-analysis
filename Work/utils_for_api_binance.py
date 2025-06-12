import requests
from pprint import pprint

import Library.utils as utils
from logger import log_error, log_warning

URL = "https://api.binance.com"
URL_SPOT = "https://api.binance.com"
URL_FUTURES_USDT = "https://fapi.binance.com"
URL_FUTURES_COIN = "https://dapi.binance.com"
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
    endpoint = "/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval
    }

    global AVAILABLE_TRADING_PAIRS
    if AVAILABLE_TRADING_PAIRS is None:
        AVAILABLE_TRADING_PAIRS = get_available_trading_pairs()

    if symbol not in AVAILABLE_TRADING_PAIRS[type_of_trading]:
        error_message = f"Торговой пары {symbol} не существует"
        log_error(error_message)
        return None

    if interval not in AVAILABLE_INTERVALS:
        error_message = f"Интервала {interval} не существует"
        log_error(error_message)
        return None

    if start is not None:
        if start < 0 or type(start) is not int:
            error_message = (
                f"Неккоректное значение времени октрытия первой свечи: "
                f"{start}"
            )
            log_error(error_message)
            return None
        params["startTime"] = start

    if end is not None:
        if end < 0 or type(end) is not int:
            error_message = (
                f"Неккоректное значение времени октрытия последней свечи: "
                f"{end}"
            )
            log_error(error_message)
            return None
        params["endTime"] = end

    if limit is not None:
        if limit < 0 or type(limit) is not int:
            error_message = (
                f"Неккоректное значение количества свечей: "
                f"{end}"
            )
            log_error(error_message)
            return None
        params["limit"] = limit

    if type_of_trading == "FUTURES" and symbol[-7] != "_":
        error_message = "Тип FUTURES требует даты справа от символа"
        log_error(error_message)
        return None

    if type_of_trading == "FUTURES" or type_of_trading == "FUTURES_PERP":

        if symbol[-4:] == "USDT" or symbol[-4:] == "USDC" or \
           symbol[-11:-7] == "USDT" or symbol[-11:-87] == "USDC":
            url_full = URL_FUTURES_USDT + "/fapi/v1/klines"
        else:
            url_full = URL_FUTURES_COIN + "/dapi/v1/klines"

    elif type_of_trading == "SPOT":
        url_full = URL_SPOT + "/api/v3/klines"
    else:
        error_message = "Такого типа торговли не существует"
        log_error(error_message)
        return None

    response = send_request_processing_params(endpoint, "GET",
                                              params, url_full)

    if "error" in response:
        error_message = (
            f"Network error in get_trading_candles:"
            f"{response['message']}"
        )
        log_error(error_message)
        # ВОТ СЮДА добавь код. ЕСЛИ это условие срабатывает,
        # то нахер выходим из функции аварийно и в тг-бота пишем мол, ошибка
        # Ее содержание доступно по response["message"]
        ...

    list_of_candles = list()
    for candle in response:
        start_time = candle[0]
        open_price = candle[1]
        high_price = candle[2]
        low_price = candle[3]
        close_price = candle[4]
        volume = candle[5]
        list_of_candles.append(
            (start_time, open_price, high_price,
                low_price, close_price, volume)
            )

    list_of_candles.sort(reverse=True)

    return list_of_candles


if __name__ == "__main__":
    print(get_trading_candles("SPOT", "BTCUSDT", "15m", limit=5))
