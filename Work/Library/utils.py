import json
from datetime import datetime

import requests


def get_api_keys_from_json(key: str):
    # Добавить проверку на существование переменнных
    with open('config.json', 'r') as file:
        data = json.load(file)
        result = data[key]

    return result


def send_request(url_full: str, method: str, params: dict, headers: dict,
                 **kwargs):
    try:
        if method.upper() == "POST":
            response = requests.request(
                method, url_full, headers=headers, data=params
                )
        elif method.upper() == "GET":
            response = requests.request(
                method, url_full, headers=headers, params=params
                )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[Ошибка сети] {e}")
        return {"error": "network", "message": str(e)}

    return response.json()
