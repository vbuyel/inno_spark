from pyspark.sql import Window
from pyspark.sql.functions import col, desc, dense_rank, count
from general_cls import SparkTask


class SparkTask5(SparkTask):
    """
    Output the top 3 actors who have appeared most in movies in the “Children” category. If several actors have the same number of movies, output all of them.
    """

    def __init__(self) -> None:
        super().__init__("task5")

    def execute(self) -> None:
        category_df = self.load_table("category")
        film_category_df = self.load_table("film_category")
        film_actor_df = self.load_table("film_actor")
        actor_df = self.load_table("actor")

        actor_movies_df = (
            category_df.where(col("name") == "Children")
            .join(film_category_df, on="category_id")
            .join(film_actor_df, on="film_id")
            .join(actor_df, on="actor_id")
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
