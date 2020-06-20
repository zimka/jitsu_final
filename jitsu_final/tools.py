import os

import requests


class TgApiClient:
    BASE_URL = 'https://api.telegram.org'

    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.base_url = f'{self.BASE_URL}/bot{token}'
        self.chat_id = chat_id

    def send_message(
        self,
        text: str,
    ):
        payload = {
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': 'Markdown',
            'disable_notification': True,
        }
        response = requests.post(
            f'{self.base_url}/sendMessage', json=payload, timeout=3
        )
        return response.json()


class SpreadSheetClient:
    pass
