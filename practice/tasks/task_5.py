from pyspark.sql import Window
from pyspark.sql.functions import col, desc, dense_rank, count, broadcast
from general_cls import SparkTask


class SparkTask5(SparkTask):
    """
    Output the top 3 actors who have appeared most in movies in the “Children” category. If several actors have the same number of movies, output all of them.
    """

    def __init__(self) -> None:
        super().__init__("task5")

    def execute(self) -> None:
        """Optimized:
        - Applied column projection pruning across all loaded tables to avoid reading unused columns.
        - Pushed down the 'Children' category filter and broadcasted the resulting single-row DataFrame to drastically prune downstream joins.
        - Broadcasted the small 'actor' dimension table (200 rows) to avoid shuffle operations during the final join.
        """
        category_df = self.load_table("category").select("category_id", "name").where(col("name") == "Children")
        film_category_df = self.load_table("film_category").select("category_id", "film_id")
        film_actor_df = self.load_table("film_actor").select("film_id", "actor_id")
        actor_df = self.load_table("actor").select("actor_id", "first_name", "last_name")

        actor_movies_df = (
            broadcast(category_df)
            .join(film_category_df, on="category_id")
            .join(film_actor_df, on="film_id")
            .join(broadcast(actor_df), on="actor_id")
            .groupBy("first_name", "last_name")
            .agg(count("film_id").alias("movie_count"))
        )

        ranking = Window.orderBy(desc("movie_count"))

        result_df = (
            actor_movies_df
            .withColumn("place", dense_rank().over(ranking))
            .where(col("place") <= 3)
            .orderBy(desc("movie_count"), col("last_name"), col("first_name"))
        )

        self.json_inload(result_df)
