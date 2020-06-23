import datetime
from airflow.operators.python_operator import PythonOperator

from .steps import get_urls_from_spreadsheet, get_urls_recently_checked, count_url_views, \
    write_urls_to_spreadsheet, log
from .model import UrlViewCheckResult


def pull_data_step(db_creds, google_spreadsheet_name, google_config_path=None):
    gs_idx, gs_urls = get_urls_from_spreadsheet(google_config_path, google_spreadsheet_name)
    db_records = get_urls_recently_checked(
        UrlViewCheckResult.get_engine_from_creds(db_creds)
    )

    for rec in db_records:
        if rec.idx in gs_idx:
            gs_row = gs_idx.index(rec.idx)
            # у нас индекс в таблице - PK
            if gs_urls[gs_urls] == rec.url:
                # не изменена
                gs_idx.pop(gs_row)
                gs_urls.pop(gs_row)
    some_old_date = datetime.datetime.now() - datetime.timedelta(days=7)
    records = [
        UrlViewCheckResult(idx=idx, url=url, result="", last_checked_at=some_old_date)
        for (idx, url) in zip(gs_idx, gs_urls)
    ]
    UrlViewCheckResult.dump_records(
        UrlViewCheckResult.get_engine_from_creds(db_creds),
        records
    )


def update_data_step(db_creds):
    db_records = get_urls_recently_checked(
        UrlViewCheckResult.get_engine_from_creds(db_creds)
    )
    urls = [rec.url for rec in db_records]
    counts = count_url_views(urls)
    now = datetime.datetime.now()
    for rec, result in zip(db_records, counts):
        rec.result = result
        rec.last_checked_at = now
    UrlViewCheckResult.dump_records(
        UrlViewCheckResult.get_engine_from_creds(db_creds),
        db_records
    )


def push_data_step(db_creds, google_spreadsheet_name, google_config_path=None):
    db_records = get_urls_recently_checked(
        UrlViewCheckResult.get_engine_from_creds(db_creds),
    )
    results = [rec.result for rec in sorted(db_records, key=lambda x: x.idx)]
    write_urls_to_spreadsheet(results, google_config_path, google_spreadsheet_name)
    # TODO - пишем в телегу


def build_dag(dag, db_creds, google_spreadsheet_name, tg_creds, google_config_path=None):

    pull_op = PythonOperator(
        python_callable=lambda x: pull_data_step(db_creds, google_spreadsheet_name, google_config_path),
        dag=dag,
        task_id='pull_op'
    )

    update_op = PythonOperator(
        python_callable=lambda x: update_data_step(db_creds),
        dag=dag,
        task_id='update_op'
    )

    push_op = PythonOperator(
        python_callable=lambda x: push_data_step(db_creds, google_spreadsheet_name, google_config_path),
        dag=dag,
        task_id='push_op'
    )
    pull_op >> update_op >> push_op