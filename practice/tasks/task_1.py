from pyspark.sql.functions import col, count, desc
from general_cls import SparkTask


class SparkTask1(SparkTask):
    def __init__(self) -> None:
        super().__init__("task1")

    def execute(self) -> None:
        category_df = self.load_table("category")
        film_category_df = self.load_table("film_category")

        result_df = (
            category_df
            .join(film_category_df, on="category_id", how="left")
            .groupBy(col("name").alias("category"))
            .agg(count("film_id").alias("movie_count"))
            .orderBy(desc("movie_count"), col("category"))
        )
        self.json_inload(result_df)
