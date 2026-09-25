from pyspark.sql.functions import col, count, desc, broadcast
from general_cls import SparkTask


class SparkTask1(SparkTask):
    """
    Output the number of movies in each category, sorted in descending order.
    """

    def __init__(self) -> None:
        super().__init__("task1")

    def execute(self) -> None:
        """Optimized:
        - Applied column projection pruning to read only necessary columns from JDBC.
        - Used broadcast join on the tiny dimension table 'category' (16 rows) to eliminate shuffle operations.
        """
        category_df = self.load_table("category").select("category_id", "name")
        film_category_df = self.load_table("film_category").select("category_id", "film_id")

        result_df = (
            film_category_df
            .join(broadcast(category_df), on="category_id", how="left")
            .groupBy(col("name").alias("category"))
            .agg(count("film_id").alias("movie_count"))
            .orderBy(desc("movie_count"), col("category"))
        )
        self.json_inload(result_df)
