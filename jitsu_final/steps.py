import datetime
from typing import Dict, List
import logging

from .parsers import UniversalViewCounter
from .model import UrlViewCheckResult, create_engine
from .tools import SpreadSheetClient, TgApiClient


log = logging.getLogger('jitsu_final')
log.setLevel(logging.INFO)
fh = logging.FileHandler('/tmp/jitsu_final.log')
fh.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
)
log.addHandler(fh)


def get_urls_from_spreadsheet(creds_path, spreadsheet_name) -> List:
    """
    Забираем список урлов из Google-таблицы
    """
    google_connect = SpreadSheetClient(creds_path)
    spreadsheet = google_connect.open_spreadsheet(spreadsheet_name)
    worksheet = spreadsheet.get_worksheet(0)
    urls = worksheet.col_values(1)[2:]
    idx = list(range(2, len(urls)))
    log.info("get_urls_from_spreadsheet done")
    return idx, urls


def write_urls_to_spreadsheet(results, creds_path, spreadsheet_name, top_left_corner='B3'):
    google_connect = SpreadSheetClient(creds_path)
    spreadsheet = google_connect.open_spreadsheet(spreadsheet_name)
    wks = spreadsheet.get_worksheet(0)
    wks.update(top_left_corner, [[r] for r in results])


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



def send_tg_report(tg_creds: Dict, urls: List, results: List):
    """
    Посылаем репорт о результатах в телеграм
    """
    log.info("send_tg_report done")


def pull_data_step(db_creds, google_spreadsheet_name, google_config_path=None):
    gs_idx, gs_urls = get_urls_from_spreadsheet(google_config_path, google_spreadsheet_name)
    db_records_all = UrlViewCheckResult.get_records(db_creds)

    for rec in db_records_all:
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
    db_records_old = UrlViewCheckResult.get_records(db_creds, 48, older=True)
    urls = [rec.url for rec in db_records_old]
    counts = count_url_views(urls)
    now = datetime.datetime.now()
    for rec, result in zip(db_records_old, counts):
        rec.result = result
        rec.last_checked_at = now
    UrlViewCheckResult.dump_records(db_creds, db_records_old)


def push_data_step(db_creds, google_spreadsheet_name, google_config_path=None):
    db_records_recent = UrlViewCheckResult.get_records(db_creds, 2, older=False),

    results = [rec.result for rec in sorted(db_records_recent, key=lambda x: x.idx)]
    write_urls_to_spreadsheet(results, google_config_path, google_spreadsheet_name)
    # TODO - пишем в телегу