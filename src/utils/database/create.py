# src/utils/database/create.py

import os
from sqlalchemy import create_engine, inspect

from utils.config import Config
from utils.database.database import Base
from utils.database.models import Location, Station

import logging

cfg = Config()

logger = logging.getLogger(__name__)

def create_database():
    logger.info("Creating database connection...")
    engine = create_engine(cfg.database_uri)
    
    # Check if tables already exist
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    if not tables:
        logger.info("No tables found. Creating database tables...")
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully.")
    else:
        logger.info("Tables already exist. Skipping table creation.")
    
    return engine

def create_tables(engine):
    logger.info("Creating database tables...")
    Base.metadata.create_all(engine)
    logger.info("Database tables created successfully.")