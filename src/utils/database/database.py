import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from utils.config import Config
from utils.database.models import Location, Station

import logging

cfg = Config()

Base = declarative_base()
logger = logging.getLogger(__name__)

def create_database_connection():
    logger.info("Creating database connection...")
    engine = create_engine(cfg.database_uri)
    return engine

def get_db_session():
    engine = create_database_connection()
    Session = sessionmaker(bind=engine)
    return Session()