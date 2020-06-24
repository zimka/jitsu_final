import json
import os

import requests
from airflow import AirflowException
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults

HOME_PATH = "/home/smirn08m/WORKSPACE/PROJECT"


class TelegramMessageSenderOperator(BaseOperator):
    @apply_defaults
    def __init__(self, bot_token: str, chat_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.headers = {"Content-Type": "application/json"}

    def execute(self, context, *args, **kwargs):
        message_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        file_url = f"https://api.telegram.org/bot{self.bot_token}/sendDocument"

        gs_urls_count = context["ti"].xcom_pull(task_ids="pull_op", key="gs_urls_count")
        last_update_urls_count = context["ti"].xcom_pull(task_ids="push_op", key="results_count")
        error_counter = context["ti"].xcom_pull(task_ids="push_op", key="error_counter")
        text = "\n".join(["🔸Smirnov-Zimka-DAG-end🔸", "", f"🔹spreadsheet URLs: {gs_urls_count}",])

        send_file = False
        if os.stat(f"{HOME_PATH}/parser_errors.txt").st_size != 0:
            text = text + "\n".join(
                [
                    "\n",
                    "🔻loading stats🔻",
                    f"🔹up-to-date URLs: {last_update_urls_count}",
                    f"🔹errors or bad URLs: {error_counter}",
                    "\n",
                    "🔻details🔻",
                ]
            )
            send_file = True
        body = {
            "chat_id": self.chat_id,
            "text": text,
        }
        response_message = requests.post(
            message_url, headers=self.headers, json=body, timeout=10, verify=False
        )
        print(response_message.text)
        if not json.loads(response_message.text)["ok"]:
            raise AirflowException("Не удалось отправить сообщение")

        if send_file:
            with open(f"{HOME_PATH}/parser_errors.txt", "rb") as txtfile:
                response_file = requests.post(
                    file_url,
                    data={"chat_id": self.chat_id},
                    files={"document": txtfile},
                    timeout=10,
                    verify=False,
                )
                print(response_file.text)
                if not json.loads(response_file.text)["ok"]:
                    raise AirflowException("Не удалось отправить файл")
