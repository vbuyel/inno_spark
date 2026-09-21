import logging
import warnings
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, desc
from tasks import SparkTask, PostgresConnector

logger = logging.getLogger(__name__)


class SparkTask1(SparkTask):
    def __init__(self) -> None:
        self.db = PostgresConnector()
        self.spark = self.create_spark_session()
        self.db.connect()

    def create_spark_session(self) -> SparkSession:
        try:
            logger.info("Creating SparkSession for Task 1...")
            spark = (
                SparkSession.builder
                .appName("Task1")
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
                .getOrCreate()
            )
            # Suppress noisy internal PySpark JVM INFO logs
            spark.sparkContext.setLogLevel("WARN")
            return spark
        except Exception:
            logger.exception("Failed to create Spark session")
            raise

    def load_table(self, table_name: str) -> DataFrame:
        logger.info(f"Loading table '{table_name}' from PostgreSQL...")
        try:
            jdbc_url = f"jdbc:postgresql://{self.db.host}:{self.db.port}/{self.db.database}"
            properties = {
                "user": self.db.user,
                "password": self.db.password,
                "driver": "org.postgresql.Driver",
            }
            df = self.spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
            logger.info(f"Successfully loaded table '{table_name}'.")
            return df
        except Exception:
            logger.exception(f"Error loading table '{table_name}'")
            raise
    
    def json_inload(self, df: DataFrame, path: str = None) -> None:
        if path is None:
            path = "practice/.results/task1"
        logger.info(f"Writing DataFrame to JSON at '{path}'...")
        try:
            df.write.mode("overwrite").json(path)
            logger.info(f"Successfully written results to '{path}'.")
        except Exception:
            logger.exception(f"Error writing DataFrame to '{path}'")
            raise

    def execute(self) -> None:
        logger.info("Executing Task 1: Count movies by category...")
        category_df = self.load_table("category")
        film_category_df = self.load_table("film_category")

        # Output the number of movies in each category, sorted in descending order
        result_df = (
            category_df
            .join(film_category_df, on="category_id", how="left")
            .groupBy(col("name").alias("category"))
            .agg(count("film_id").alias("movie_count"))
            .orderBy(desc("movie_count"), col("category"))
        )
        self.json_inload(result_df)
        logger.info("Task 1 execution finished.")

    def close_all(self) -> None:
        logger.info("Closing SparkSession and database connections...")
        self.spark.stop()
        self.db.disconnect()
