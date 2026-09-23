from pyspark.sql.functions import broadcast
from general_cls import SparkTask


class SparkTask4(SparkTask):
    """
    Output the names of movies that are not in the inventory.
    """

    def __init__(self) -> None:
        super().__init__("task4")

    def execute(self) -> None:
        """Optimized:
        - Replaced 'left' join + isNull filter with a 'left_anti' join to short-circuit matching and eliminate unnecessary null row generation and post-filtering.
        - Applied column projection and deduplication on inventory film IDs.
        - Used broadcast join on the inventory table to execute an in-memory hash anti-join without shuffling.
        """
        film_df = self.load_table("film").select("film_id", "title")
        inventory_df = self.load_table("inventory").select("film_id").dropDuplicates(["film_id"])

        result_df = (
            film_df
            .join(broadcast(inventory_df), on="film_id", how="left_anti")
            .select("title")
        )
        self.json_inload(result_df)
