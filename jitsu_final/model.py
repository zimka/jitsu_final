from contextlib import contextmanager
import datetime

from sqlalchemy import create_engine
from sqlalchemy import Column, DateTime, String
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

    url = Column('url', String(), primary_key=True)
    result = Column('result', String(400))
    last_checked_at = Column('last_checked_at', DateTime(), default=datetime.datetime.now)

    @classmethod
    def init(cls, engine):
        Base.metadata.create_all(engine)

    @classmethod
    def dump_results(cls, engine, urls, results):
        with session_scope(engine) as session:
            for u,r in zip(urls, results):
                obj = cls(url=u, result=str(r))
                session.merge(obj)

    @classmethod
    def get_recently_checked_urls(cls, engine, recent_days=2):
        recent_date = datetime.datetime.now() - datetime.timedelta(days=recent_days)
        with session_scope(engine) as session:
            query = session.query(cls).filter(
                cls.last_checked_at > recent_date
            )
            return [record.url for record in query.all()]
