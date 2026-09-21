import warnings
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, desc

from tasks import SparkTask, PostgresConnector


class SparkTask1(SparkTask):
    def __init__(self) -> None:
        self.db = PostgresConnector()
        self.spark = self.create_spark_session()
        self.db.connect()

    def create_spark_session(self) -> SparkSession:
        return (
            SparkSession.builder
            .appName("Task1")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .getOrCreate()
        )

    def load_table(self, table_name: str) -> DataFrame:
        try:
            jdbc_url = f"jdbc:postgresql://{self.db.host}:{self.db.port}/{self.db.database}"
            properties = {
                "user": self.db.user,
                "password": self.db.password,
                "driver": "org.postgresql.Driver",
            }
            return self.spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
        except Exception:
            import pandas as pd
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
                pdf = pd.read_sql(f"SELECT * FROM {table_name}", self.db.conn)
            return self.spark.createDataFrame(pdf)

    def execute(self) -> None:
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

        result_df.show(truncate=False)

    def close_all(self) -> None:
        self.spark.stop()
        self.db.disconnect()
