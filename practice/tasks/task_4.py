from pyspark.sql.functions import col
from general_cls import SparkTask


class SparkTask4(SparkTask):
    """
    Output the names of movies that are not in the inventory.
    """

    def __init__(self) -> None:
        super().__init__("task4")

    def execute(self) -> None:
        film_df = self.load_table("film")
        inventory_df = self.load_table("inventory")

        result_df = (
            film_df.select("film_id", "title")
            .join(inventory_df.select("film_id", "inventory_id"), on="film_id", how="left")
            .where(col("inventory_id").isNull())
        ).select("title")
        self.json_inload(result_df)
