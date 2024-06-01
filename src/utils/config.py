import os

class Config:
    def __init__(self):
        self.db_name = os.getenv('POSTGRES_DBNAME', 'velib')
        self.db_schema = os.getenv('POSTGRES_SCHEMA', 'public')
        self.db_user = os.getenv('POSTGRES_USER', 'velib_user')
        self.db_password = os.getenv('POSTGRES_PASSWORD', 'velib_password')
        self.db_host = os.getenv('POSTGRES_HOST', 'user-postgres')
        self.db_port = os.getenv('POSTGRES_PORT', '5432')

    @property
    def database_uri(self):
        return f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}'