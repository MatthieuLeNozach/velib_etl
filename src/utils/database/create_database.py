# src/utils/database/create_database.py

import os
from sqlalchemy import create_engine, inspect
from utils.config import Config
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
        logger.info("No tables found.")
    else:
        logger.info("Tables already exist.")
    
    return engine

if __name__ == "__main__":
    create_database()