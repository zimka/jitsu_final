import uuid
from sqlalchemy import create_engine
from .model import UrlViewCheckResult

test_engine = create_engine(
    'sqlite:////tmp/airflow_tests_{}.db'.format(uuid.uuid4()),
    echo=True
)


def test_save_n_load():
    idxs = [1, 5, 42]
    urls = ['http://ya.ru', 'https://vk.com', 'https://airflow.apache.org']
    results = ['Failed', 30, 50]

    UrlViewCheckResult.init(test_engine)
    records = UrlViewCheckResult.get_recently_checked_urls(test_engine, recent_hours=10000)
    assert len(records) == 0
    UrlViewCheckResult.dump_results(
        test_engine, idxs, urls, results
    )
    records = UrlViewCheckResult.get_recently_checked_urls(test_engine, recent_hours=10000)
    assert len(records) == len(idxs)
    records = UrlViewCheckResult.get_recently_checked_urls(test_engine, recent_hours=0)
    assert len(records) == 0
    UrlViewCheckResult.dump_results(test_engine, [142], ['rtsp://broken_page'], 'Failed')
    records = UrlViewCheckResult.get_recently_checked_urls(test_engine, recent_hours=4)
    assert len(records) == len(idxs) + 1
    UrlViewCheckResult.dump_results(
        test_engine, idxs[0:1], urls[0:1], ['Now success!']
    )
    records = UrlViewCheckResult.get_recently_checked_urls(test_engine, recent_hours=4)
    assert len(records) == len(idxs) + 1
