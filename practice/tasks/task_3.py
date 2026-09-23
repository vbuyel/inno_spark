from pyspark.sql.functions import col, desc, sum
from general_cls import SparkTask


class SparkTask3(SparkTask):
    """
    Output the category of movies on which the most money was spent. 
    """

    def __init__(self) -> None:
        super().__init__("task3")

    def execute(self) -> None:
        payment_df = self.load_table("payment")
        rental_df = self.load_table("rental")
        inventory_df = self.load_table("inventory")
        film_category_df = self.load_table("film_category")
        category_df = self.load_table("category")

        result_df = (
            payment_df.select("rental_id", "amount")
            .join(rental_df.select("rental_id", "inventory_id"), on="rental_id", how="inner")
            .join(inventory_df.select("inventory_id", "film_id"), on="inventory_id", how="inner")
            .join(film_category_df.select("film_id", "category_id"), on="film_id", how="inner")
            .join(category_df.select("category_id", "name"), on="category_id", how="inner")
            .groupBy(col("name").alias("category"))
            .agg(sum("amount").alias("total_spent"))
            .orderBy(desc("total_spent"))
            .limit(1)
        )
        self.json_inload(result_df)
