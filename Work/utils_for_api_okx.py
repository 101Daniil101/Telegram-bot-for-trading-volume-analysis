import hmac
import hashlib
import base64
import json
from datetime import datetime
from pprint import pprint

import Library.utils as utils
from logger import log_error, log_warning

API_KEY = utils.get_api_keys_from_json("API_KEY_OKX")
SECRET_KEY = utils.get_api_keys_from_json("API_SECRET_KEY_OKX")
API_PASSPHRASE = utils.get_api_keys_from_json("API_PASSPHRASE")
URL = "https://www.okx.com"
AVAILABLE_INTERVALS = ("1m", "3m", "5m", "15m", "30m", "1H", "2H", "4H",
                       "6H", "12H", "1D", "2D", "3D", "1W", "1M", "3M")  # Внимательнее, возможно UTC +8
AVAILABLE_TRADING_PAIRS = None

# Разберись в общем с логированием и выводом ошибок

def get_okx_timestamp() -> str:
    now = datetime.utcnow()
    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def get_sign(timestamp: str, method: str,
             request_path: str, payload: str) -> str:

    pre_hash_string = timestamp + method.upper() + request_path + payload
    signature = hmac.new(
        SECRET_KEY.encode('utf-8'),
        pre_hash_string.encode('utf-8'),
        hashlib.sha256
    ).digest()

    return base64.b64encode(signature).decode()


def send_request_processing_params(endpoint, method, params):
    timestamp = get_okx_timestamp()

    if method == "POST":
        payload = json.dumps(params, separators=(',', ':'))
    elif method == "GET":
        payload = "?" + "&".join([f"{k}={v}" for k, v in params.items()])

    headers = {
        'OK-ACCESS-SIGN': get_sign(timestamp, method, endpoint, payload),
        'OK-ACCESS-TIMESTAMP': timestamp,
        'OK-ACCESS-KEY': API_KEY,
        'OK-ACCESS-PASSPHRASE': API_PASSPHRASE,
        'Content-Type': 'application/json'
    }

    url_full = URL + endpoint

    response = utils.send_request(url_full, method, params, headers = {})

    return response


def get_trading_candles(instId: str, bar: str,
                               after: str=None,
                               before: str=None, limit: str=None):

    global AVAILABLE_TRADING_PAIRS
    if AVAILABLE_TRADING_PAIRS is None:
        AVAILABLE_TRADING_PAIRS = get_available_trading_pairs()

    if ("SWAP" in instId):
        if (instId not in AVAILABLE_TRADING_PAIRS["SWAP"]):
            error_message = f"Торговой пары {instId} не существует"
            log_error(error_message)
            return None
    elif (instId.count("-") == 2):
        if (instId not in AVAILABLE_TRADING_PAIRS["FUTURES"]):
            error_message = f"Торговой пары {instId} не существует"
            log_error(error_message)
            return None
    else:
        if (instId not in AVAILABLE_TRADING_PAIRS["SPOT"]):
            error_message = f"Торговой пары {instId} не существует"
            log_error(error_message)
            return None

    if bar not in AVAILABLE_INTERVALS:
        error_message = f"Интервала {bar} не существует"
        log_error(error_message)
        return None

    endpoint = "/api/v5/market/candles"
    params = {
        "instId": instId,
        "bar": bar
    }

    if after is not None:
        if int(after) < 0 or int(after) < int(before):
            error_message = (
                f"Неккоректное значение времени октрытия последней свечи: "
                f"{after}"
            )
            log_error(error_message)
            return None
        params["after"] = str(after)

    if before is not None:
        if int(before) < 0 or int(before) > int(after):
            error_message = (
                f"Неккоректное значение времени октрытия первой свечи: "
                f"{after}"
            )
            log_error(error_message)
            return None
        params["before"] = str(before)

    if limit is not None:
        if int(limit) < 0:
            error_message = (
                f"Неккоректное значение количества свечей: "
                f"{limit}"
            )
            log_error(error_message)
            return None
        params["limit"] = limit

    response = send_request_processing_params(endpoint, "GET", params)

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
    for candle in response["data"]:
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
    
    return list_of_candles


def get_available_trading_pairs():
    endpoint = "/api/v5/public/instruments"
    trading_pairs = dict()

    params = {"instType": "SPOT"}
    response = send_request_processing_params(endpoint, "GET", params)
    trading_pairs["SPOT"] = [
        trading_pair["instId"] for trading_pair in response["data"]
        ]

    params = {"instType": "SWAP"}
    response = send_request_processing_params(endpoint, "GET", params)
    trading_pairs["SWAP"] = [
        trading_pair["instId"] for trading_pair in response["data"]
        ]

    params = {"instType": "FUTURES"}
    response = send_request_processing_params(endpoint, "GET", params)
    trading_pairs["FUTURES"] = [
        trading_pair["instId"] for trading_pair in response["data"]
        ]

    return trading_pairs
