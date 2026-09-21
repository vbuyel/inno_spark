from pyspark.sql.functions import count, desc, sum
from general_cls import SparkTask


class SparkTask2(SparkTask):
    def __init__(self) -> None:
        super().__init__("task2")

    def execute(self) -> None:
        rental_df = self.load_table("rental")
        inventory_df = self.load_table("inventory")
        actor_df = self.load_table("actor")
        film_actor_df = self.load_table("film_actor")
        
        inventory_rental_df = (
            inventory_df
            .join(rental_df, on="inventory_id", how="inner")
            .groupBy("film_id")
            .agg(count("rental_id").alias("rental_count"))
        )

        film_actor_renatal_df = (
            film_actor_df
            .join(inventory_rental_df, on="film_id", how="left")
        )

        actor_rental_df = (
            actor_df
            .join(film_actor_renatal_df, on="actor_id", how="left")
            .groupBy("first_name", "last_name")
            .agg(sum("rental_count").alias("rental_count"))
            .orderBy(desc("rental_count"))
            .limit(10)
        )

        self.json_inload(actor_rental_df)
