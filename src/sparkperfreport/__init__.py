from sqlalchemy import engine_from_config
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from zope.sqlalchemy import register


Base = declarative_base()

settings = {
    'sqlalchemy.url': 'sqlite:///1.db'
}

engine = engine_from_config(settings, 'sqlalchemy.')

DBSession = scoped_session(sessionmaker(bind=engine, autoflush=False))
register(DBSession)
