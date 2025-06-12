import time
import hmac
import hashlib
import json

import Library.utils as utils

API_KEY = utils.get_api_keys_from_json("API_KEY_BYBIT")
SECRET_KEY = utils.get_api_keys_from_json("API_SECRET_KEY_BYBIT")
URL = "https://api.bybit.com"
RECV_WINDOW = str(5000)
AVAILABLE_INTERVALS = ("1", "3", "5", "15", "30", "60", "120",
                       "240", "360", "720", "D", "W", "M")
AVAILABLE_TRADING_PAIRS = None

# Разберись в общем с логированием и выводом ошибок


def gen_signature(payload: str, time_stamp: int):
    param_str = str(time_stamp) + API_KEY + RECV_WINDOW + payload
    hash = hmac.new(
        bytes(SECRET_KEY, "utf-8"), param_str.encode("utf-8"), hashlib.sha256
        )
    signature = hash.hexdigest()
    return signature


def send_request_processing_params(endpoint, method, params):
    time_stamp = str(int(time.time() * 10 ** 3))

    if method == "POST":
        payload = json.dumps(params, separators=(',', ':'))
    else:
        payload = "&".join([f"{k}={v}" for k, v in params.items()])

    signature = gen_signature(payload, time_stamp)

    headers = {
        'X-BAPI-API-KEY': API_KEY,
        'X-BAPI-SIGN': signature,
        'X-BAPI-SIGN-TYPE': '2',
        'X-BAPI-TIMESTAMP': time_stamp,
        'X-BAPI-RECV-WINDOW': RECV_WINDOW,
        'Content-Type': 'application/json'
    }

    url_full = URL + endpoint

    response = utils.send_request(url_full, method, params, headers = {})

    return response


def get_trading_candles(category: str, symbol: str,
                               interval: str, start: int = None,
                               end: int = None, limit: int = None):
    """start and end get params in ms"""

    global AVAILABLE_TRADING_PAIRS
    if AVAILABLE_TRADING_PAIRS is None:
        AVAILABLE_TRADING_PAIRS = get_available_trading_pairs()

    if category not in ('spot', 'linear', 'inverse'):
        print("Такого типа торгов не существует ") 
        return

    if symbol not in AVAILABLE_TRADING_PAIRS['SPOT'] and \
       symbol not in AVAILABLE_TRADING_PAIRS['FUTURES']:
        print("Такой торговой пары не существует")
        return

    if interval not in AVAILABLE_INTERVALS:
        print("Такого интервала не существует")
        return

    endpoint = "/v5/market/kline"
    params = {
        "category": category,
        "symbol": symbol,
        "interval": interval
    }

    if start is not None:
        if start < 0 or start > end:
            print("")
            return
        params["start"] = start

    if end is not None:
        if end < 0 or end < start:
            print("")
            return
        params["end"] = end

    if limit is not None:
        if limit < 0:
            print("")
            return
        params["limit"] = limit

    response = send_request_processing_params(endpoint, "GET", params)

    list_of_candles = list()  # Добавь цвет свечей, если нужно
    if response['retMsg'] == 'OK':
        for candle in response['result']['list']:
            start_time = candle[0]  # In ms
            open_price = candle[1]
            high_price = candle[2]
            low_price = candle[3]
            close_price = candle[4]
            volume = candle[5]  # In basecoin, but also user use inverse contract...
            list_of_candles.append(
                (start_time, open_price, high_price,
                 low_price, close_price, volume)
                )
    else:
        print("")
        return

    return list_of_candles


def get_available_trading_pairs():
    endpoint = '/v5/market/instruments-info'
    trading_pairs = dict()

    params = {'category': 'spot'}
    response = send_request_processing_params(endpoint, "GET", params)
    trading_pairs['SPOT'] = [
        pair['symbol'] for pair in response['result']['list']
    ]

    params = {'category': 'linear'}
    response = send_request_processing_params(endpoint, "GET", params)
    trading_pairs['FUTURES'] = [
        pair['symbol'] for pair in response['result']['list']
    ]

    return trading_pairs


if __name__ == "__main__":
    endpoint = '/v5/account/wallet-balance'
    params = {"accountType": "UNIFIED"}

    print(get_trading_candles("spot", "BTCUSDT", "15", limit=2))
