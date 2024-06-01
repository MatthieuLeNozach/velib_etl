# src/utils/database/create_tables.py

from sqlalchemy import create_engine
from src.utils.config import Config
from src.utils.database.database import Base
from src.utils.database.models import Location, Station
import logging

cfg = Config()
logger = logging.getLogger(__name__)

def create_tables():
    logger.info("Creating database connection...")
    engine = create_engine(cfg.database_uri)
    
    logger.info("Creating database tables...")
    Base.metadata.create_all(engine)
    logger.info("Database tables created successfully.")

if __name__ == "__main__":
    create_tables()