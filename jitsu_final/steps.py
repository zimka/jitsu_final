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


def get_urls_recently_checked(db_engine, hours=48):
    """
    Забираем из таблицы список уже проверенных
    недавно урлов
    """
    UrlViewCheckResult.init(db_engine)
    urls = UrlViewCheckResult.get_recently_checked_urls(db_engine, recent_hours=hours)
    log.info("get_urls_recently_checked done")
    return urls


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


def dump_results_to_db(db_engine, idxs:List, urls: List, results: List) -> None:
    """
    Сохраняем результаты проверки в базу данных
    """
    UrlViewCheckResult.dump_results(db_engine, idxs, urls, results)
    log.info("dump_results_to_db done")


def dump_results_to_spreadsheet(google_sheets_creds: Dict, urls: List, results: List):
    """
    Сохраняем результаты проверки в Google-таблицу
    """
    log.info("dump_results_to_spreadsheet done")


def send_tg_report(tg_creds: Dict, urls: List, results: List):
    """
    Посылаем репорт о результатах в телеграм
    """
    log.info("send_tg_report done")


def run_view_count(db_creds, google_sheets_creds, tg_creds):
    """
    Выполняет end-to-end подсчет ссылок и обработку результатов
    Args:
        db_creds: connection_string или sqlalchemy-engine для доступа к базе
        google_spreadsheet_creds (dict): словарь с creds для доступа к Google-табличке
        tg_creds (dict): словарь с creds для доступа к телеграм боту
    Returns:
        None
    """
    if isinstance(db_creds, str):
        db_engine = create_engine(db_creds)
    else:
        db_engine = db_creds

    all_urls = get_urls_from_spreadsheet(google_sheets_creds)
    recently_checked_urls = get_urls_recently_checked(db_engine)

    urls_to_check = [
        u for u in all_urls
        if u not in recently_checked_urls
    ]
    counts = count_url_views(urls_to_check)
    dump_results_to_db(db_engine, urls_to_check, counts)
    dump_results_to_spreadsheet(google_sheets_creds, urls_to_check, counts)
    send_tg_report(tg_creds, urls_to_check, counts)
