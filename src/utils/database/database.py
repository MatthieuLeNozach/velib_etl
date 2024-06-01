# database.py

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

def create_database():
    logger.info("Creating database connection...")
    engine = create_engine(cfg.database_uri)
    Base.metadata.create_all(engine)
    logger.info("Database connection created successfully.")
    return engine

def get_db_session():
    engine = create_database()
    Session = sessionmaker(bind=engine)
    return Session()

def create_tables(engine):
    logger.info("Creating database tables...")
    Base.metadata.create_all(engine)
    logger.info("Database tables created successfully.")
