from contextlib import contextmanager
import datetime

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, DateTime, String
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

@contextmanager
def session_scope(engine):
    """Рекомендуемый документацией метод работы с сессией"""
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class UrlViewCheckResult(Base):
    __tablename__ = "url_view_check_result"
    idx = Column('idx', Integer(), primary_key=True)
    url = Column('url', String())
    result = Column('result', String(400))
    last_checked_at = Column('last_checked_at', DateTime(), default=datetime.datetime.now)

    @classmethod
    def init(cls, engine):
        Base.metadata.create_all(engine)

    @classmethod
    def get_engine_from_creds(cls, creds):
        return create_engine(creds)

    @classmethod
    def dump_results(cls, engine, idxs, urls, results):
        """
        Создает либо перезаписывает в базе записи с измерением
        колчества просмотров
        """
        records = []
        for idx, u, r in zip(idxs, urls, results):
            obj = cls(idx=idx, url=u, result=str(r))
            records.append(obj)
        cls.dump_records(engine, records)

    @classmethod
    def dump_records(cls, engine, records):
        """
        Создает либо перезаписывает в базе записи с измерением
        колчества просмотров
        """
        with session_scope(engine) as session:
            for rec in records:
                session.merge(rec)

    @classmethod
    def get_recently_checked_urls(cls, engine, recent_hours=48):
        recent_date = datetime.datetime.now() - datetime.timedelta(hours=recent_hours)
        with session_scope(engine) as session:
            query = session.query(cls)
            if recent_hours is not None:
                query = query.filter(
                cls.last_checked_at > recent_date
            )
            return list(query.all())
