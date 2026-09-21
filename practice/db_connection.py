import os
import psycopg2
from dotenv import load_dotenv


load_dotenv()

class PostgresConnector:
    def __init__(
        self,
        host=os.getenv("DB_HOST"), 
        port=os.getenv("DB_PORT"), 
        database=os.getenv("DB_DATABASE"), 
        user=os.getenv("DB_USER"), 
        password=os.getenv("DB_PASSWORD")
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

    def connect(self) -> psycopg2.connect:
        """Create connection to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            return self.conn
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            return None

    def disconnect(self) -> None:
        """Close connection to PostgreSQL database"""
        if self.conn:
            self.conn.close()
            self.conn = None
