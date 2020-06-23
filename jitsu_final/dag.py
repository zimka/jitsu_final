import datetime
from airflow.operators.python_operator import PythonOperator

from .steps import pull_data_step, update_data_step, push_data_step
from .model import UrlViewCheckResult


def build_dag(dag, db_creds, google_spreadsheet_name, tg_creds, google_config_path=None):

    pull_op = PythonOperator(
        python_callable=lambda: pull_data_step(db_creds, google_spreadsheet_name, google_config_path),
        dag=dag,
        task_id='pull_op'
    )

    update_op = PythonOperator(
        python_callable=lambda: update_data_step(db_creds),
        dag=dag,
        task_id='update_op'
    )

    push_op = PythonOperator(
        python_callable=lambda: push_data_step(db_creds, google_spreadsheet_name, google_config_path),
        dag=dag,
        task_id='push_op'
    )
    pull_op >> update_op >> push_op