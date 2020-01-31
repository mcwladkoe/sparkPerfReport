import sys

from sqlalchemy import create_engine
from . import DBSession, Base
from .models import SparkPerfClusterTest, SparkPerfTestPack, SparkPerfTestResult


def main(argv=sys.argv):
    # config_uri = argv[1]

    engine = create_engine('sqlite:///1.db')
    DBSession.configure(bind=engine)
    Base.metadata.bind = engine
    Base.metadata.create_all(engine)
