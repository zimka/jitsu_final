import datetime
import logging
from typing import List

from .model import UrlViewCheckResult
from .parsers import UniversalViewCounter
from .tools import SpreadSheetClient

log = logging.getLogger("jitsu_final")
log.setLevel(logging.INFO)
fh = logging.FileHandler("/tmp/jitsu_final.log")
fh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
log.addHandler(fh)


HOME_PATH = "/home/smirn08m/WORKSPACE/PROJECT"


def get_urls_from_spreadsheet(creds_path: str, spreadsheet_name: str) -> List:
    """
    Забираем список урлов из Google-таблицы
    """
    google_connect = SpreadSheetClient(creds_path)
    spreadsheet = google_connect.open_spreadsheet(spreadsheet_name)
    worksheet = spreadsheet.get_worksheet(0)
    urls = worksheet.col_values(1)[2:]
    log.info(len(urls))
    idx = list(range(3, len(urls) + 3))
    log.info("get_urls_from_spreadsheet done")
    return idx, urls


def write_urls_to_spreadsheet(
    results: List, creds_path: str, spreadsheet_name: str, top_left_corner="B3"
):
    google_connect = SpreadSheetClient(creds_path)
    spreadsheet = google_connect.open_spreadsheet(spreadsheet_name)
    worksheet = spreadsheet.get_worksheet(0)
    worksheet.update(top_left_corner, [[r] for r in results])


def count_url_views(urls: List) -> List:
    """
    Возвращает количество просмотров либо текст ошибки
    для всех урлов в списке
    """
    counter = UniversalViewCounter()
    view_counts = []
    for u in urls:
        view_counts.append(counter.get_count_views_message(u))
    counter.quit()
    log.info("count_url_views done")
    return view_counts


def telegram_file_report_generation(true_result: List, *args, **context):
    """
    Создание файла с ошибками для репорта в телеграм
    """
    error_counter = 0
    with open(f"{HOME_PATH}/parser_errors.txt", "w") as txtfile:
        for one_result in true_result:
            try:
                int(one_result[1])
            except ValueError:
                txtfile.write(", ".join(str(s) for s in (one_result[0], one_result[2])) + "\n")
                error_counter += 1
        print(error_counter)
        context["task_instance"].xcom_push(key="error_counter", value=error_counter)
    log.info("tg_file_report done")


def pull_data_step(
    db_creds: str, google_spreadsheet_name: str, google_config_path=None, *args, **context
):
    gs_idx, gs_urls = get_urls_from_spreadsheet(google_config_path, google_spreadsheet_name)
    db_records_all = UrlViewCheckResult.get_records(db_creds)
    context["task_instance"].xcom_push(key="gs_urls_count", value=len(gs_idx))

    for rec in db_records_all:
        if rec.idx in gs_idx:
            gs_row = gs_idx.index(rec.idx)
            # у нас индекс в таблице - PK
            if gs_urls[gs_row] == rec.url:
                # не изменена
                gs_idx.pop(gs_row)
                gs_urls.pop(gs_row)
    some_old_date = datetime.datetime.now() - datetime.timedelta(days=7)
    records = [
        UrlViewCheckResult(idx=idx, url=url, result="", last_checked_at=some_old_date)
        for (idx, url) in zip(gs_idx, gs_urls)
    ]
    UrlViewCheckResult.dump_records(UrlViewCheckResult.get_engine_from_creds(db_creds), records)


def update_data_step(db_creds: str):
    db_records_old = UrlViewCheckResult.get_records(db_creds, hours=48, older=True)
    urls = [rec.url for rec in db_records_old]
    counts = count_url_views(urls)
    now = datetime.datetime.now()
    for rec, result in zip(db_records_old, counts):
        rec.result = result
        rec.last_checked_at = now
    UrlViewCheckResult.dump_records(db_creds, db_records_old)


def push_data_step(
    db_creds: str, google_spreadsheet_name: str, google_config_path=None, *args, **context
):
    db_records_recent = (UrlViewCheckResult.get_records(db_creds, hours=1, older=False),)
    rec_idx = []
    rec_result = []
    rec_url = []
    for rec in db_records_recent:
        for one in rec:
            rec_idx.append(one.idx)
            rec_result.append(one.result)
            rec_url.append(one.url)
    records_result = list(zip(rec_idx, rec_result, rec_url))
    true_result = sorted(records_result, key=lambda x: x[0])
    results = [result[1] for result in true_result]
    print(results)
    telegram_file_report_generation(true_result, *args, **context)
    write_urls_to_spreadsheet(results, google_config_path, google_spreadsheet_name)
    context["task_instance"].xcom_push(key="results_count", value=len(results))
