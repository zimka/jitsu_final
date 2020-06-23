import os

import gspread
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
    #DEFAULT_CREDENTIALS_PATH = '~/.config/gspread/service_account.json'

    def __init__(self, creds_path=None):
        #if creds_path is None:
        #    creds_path = self.DEFAULT_CREDENTIALS_PATH
        if creds_path is not None:
            self.gc = gspread.service_account(filename=creds_path)
        else:
            self.gc = gspread.service_account()

    def open_spreadsheet(self, spreadsheet_name: str):
        self.sh = self.gc.open(spreadsheet_name)
        return self.sh

