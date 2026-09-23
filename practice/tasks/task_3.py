from pyspark.sql.functions import col, desc, sum, broadcast
from general_cls import SparkTask


class SparkTask3(SparkTask):
    """
    Output the category of movies on which the most money was spent. 
    """

    def __init__(self) -> None:
        super().__init__("task3")

    def execute(self) -> None:
        """Optimized:
        - Applied column projection pruning at load time to transfer only necessary fields over JDBC.
        - Used broadcast joins on dimension tables ('inventory', 'film_category', 'category') to eliminate shuffle stages across multi-table joins.
        """
        payment_df = self.load_table("payment").select("rental_id", "amount")
        rental_df = self.load_table("rental").select("rental_id", "inventory_id")
        inventory_df = self.load_table("inventory").select("inventory_id", "film_id")
        film_category_df = self.load_table("film_category").select("film_id", "category_id")
        category_df = self.load_table("category").select("category_id", "name")

        result_df = (
            payment_df
            .join(rental_df, on="rental_id", how="inner")
            .join(broadcast(inventory_df), on="inventory_id", how="inner")
            .join(broadcast(film_category_df), on="film_id", how="inner")
            .join(broadcast(category_df), on="category_id", how="inner")
            .groupBy(col("name").alias("category"))
            .agg(sum("amount").alias("total_spent"))
            .orderBy(desc("total_spent"))
            .limit(1)
        )
        self.json_inload(result_df)
