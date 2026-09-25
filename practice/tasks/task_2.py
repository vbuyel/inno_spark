from pyspark.sql.functions import count, desc, sum, broadcast
from general_cls import SparkTask


class SparkTask2(SparkTask):
    """
    Output the 10 actors whose movies rented the most, sorted in descending order.
    """

    def __init__(self) -> None:
        super().__init__("task2")

    def execute(self) -> None:
        """Optimized:
        - Applied column projection pruning on all JDBC tables to minimize I/O and network transfer.
        - Used broadcast joins on dimension tables ('inventory', 'actor', 'film_actor') to eliminate unnecessary shuffles against the large rental table.
        - Pre-aggregated rental counts at the film level before joining with actor dimensions to reduce intermediate shuffle volume.
        """
        rental_df = self.load_table("rental").select("inventory_id")
        inventory_df = self.load_table("inventory").select("inventory_id", "film_id")
        actor_df = self.load_table("actor").select("actor_id", "first_name", "last_name")
        film_actor_df = self.load_table("film_actor").select("actor_id", "film_id")

        # actor_id | rental_count
        actor_rental_df = (
            rental_df
            .join(broadcast(inventory_df), on="inventory_id", how="inner")
            .join(broadcast(film_actor_df), on="film_id", how="inner")
            .groupBy("actor_id")
            .agg(count("*").alias("rental_count"))
        )

        # first_name | last_name | rental_count
        actor_rental_df = (
            actor_rental_df
            .join(broadcast(actor_df), on="actor_id", how="left")
            .groupBy("first_name", "last_name")
            .agg(sum("rental_count").alias("rental_count"))
            .orderBy(desc("rental_count"))
            .limit(10)
        )

        self.json_inload(actor_rental_df)
