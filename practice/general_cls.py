import logging
from pyspark.sql import DataFrame, SparkSession
from db_connection import PostgresConnector

logger = logging.getLogger(__name__)


class SparkTask:
    def __init__(self, task_name: str) -> None:
        self.task_name = task_name
        self.db = PostgresConnector()
        self.spark = self.create_spark_session()
        self.db.connect()

    def create_spark_session(self) -> SparkSession:
        try:
            logger.info(f"{self.task_name}: Creating SparkSession...")
            spark = (
                SparkSession.builder
                .appName(self.task_name)
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
                .getOrCreate()
            )
            # Suppress noisy internal PySpark JVM INFO logs
            spark.sparkContext.setLogLevel("WARN")
            return spark
        except Exception:
            logger.exception(f"{self.task_name}: Failed to create Spark session")
            raise

    def load_table(self, table_name: str) -> DataFrame:
        logger.info(f"{self.task_name}: Loading table '{table_name}' from PostgreSQL...")
        try:
            jdbc_url = f"jdbc:postgresql://{self.db.host}:{self.db.port}/{self.db.database}"
            properties = {
                "user": self.db.user,
                "password": self.db.password,
                "driver": "org.postgresql.Driver",
            }
            df = self.spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
            logger.info(f"{self.task_name}: Successfully loaded table '{table_name}'.")
            return df
        except Exception:
            logger.exception(f"{self.task_name}: Error loading table '{table_name}'")
            raise
    
    def json_inload(self, df: DataFrame, path: str = None) -> None:
        if path is None:
            path = f"practice/.results/{self.task_name}"
        logger.info(f"{self.task_name}: Writing DataFrame to JSON at '{path}'...")
        try:
            df.write.mode("overwrite").json(path)
            logger.info(f"{self.task_name}: Successfully written results to '{path}'.")
        except Exception:
            logger.exception(f"{self.task_name}: Error writing DataFrame to '{path}'")
            raise

    def close_all(self) -> None:
        logger.info(f"{self.task_name}: Closing SparkSession and database connections...")
        self.spark.stop()
        self.db.disconnect()

    def execute(self) -> None:
        pass
