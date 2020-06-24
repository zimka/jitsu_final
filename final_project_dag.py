import datetime

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from jitsu_final.operators import TelegramMessageSenderOperator
from jitsu_final.steps import pull_data_step, push_data_step, update_data_step

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


db_creds = "postgresql+psycopg2://jitsu:jitsu@127.0.0.1:5432/jitsu"
google_spreadsheet_name = "airflow101.экселька"
google_config_path = None


HOME_PATH = "/home/smirn08m/WORKSPACE/PROJECT"
BOT_TOKEN = Variable.get("BOT_TOKEN")
CHAT_ID = Variable.get("CHAT_ID")


default_args = {
    "owner": "Smirnov, Zimka",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 6, 22, 0, 0),
    "catchup": False,
}


with DAG(
    "project_homework",
    default_args=default_args,
    description="Collects and dumps urls view data from different google table.",
    schedule_interval="0 1 * * *",
) as dag:

    task_pull_op = PythonOperator(
        python_callable=pull_data_step,
        op_kwargs={
            "db_creds": db_creds,
            "google_spreadsheet_name": google_spreadsheet_name,
            "google_config_path": google_config_path,
        },
        provide_context=True,
        task_id="pull_op",
        dag=dag,
    )

    task_update_op = PythonOperator(
        task_id="update_op", python_callable=lambda: update_data_step(db_creds), dag=dag
    )

    task_push_op = PythonOperator(
        python_callable=push_data_step,
        op_kwargs={
            "db_creds": db_creds,
            "google_spreadsheet_name": google_spreadsheet_name,
            "google_config_path": google_config_path,
        },
        provide_context=True,
        task_id="push_op",
        dag=dag,
    )

    task_telegram_final_allert = TelegramMessageSenderOperator(
        task_id="telegram_result_allert",
        bot_token=BOT_TOKEN,
        chat_id=CHAT_ID,
        provide_context=True,
        dag=dag,
    )

task_pull_op >> task_update_op >> task_push_op >> task_telegram_final_allert
