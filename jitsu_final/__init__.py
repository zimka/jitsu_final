from airflow.operators.python_operator import PythonOperator

from .steps import run_view_count


def build_dag(dag, db_creds, google_spreadsheet_creds, tg_creds):
    end2end_run_views_count_step = PythonOperator(
        task_id='end2end_run_views_count_step',
        python_callable=lambda: run_views_count(
            db_creds,
            google_spreadsheet_creds,
            tg_creds
        ),
        dag=dag
    )

