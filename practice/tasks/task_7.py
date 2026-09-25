from pyspark.sql import Window
from pyspark.sql.functions import col, sum, desc, dense_rank, broadcast
from general_cls import SparkTask


class SparkTask7(SparkTask):
    """
    Output the category of movies that have the highest number of total rental hours in the cities (customer.address_id in this city), and that start with the letter "a". Do the same for cities with a "-" symbol.
    """

    def __init__(self) -> None:
        super().__init__("task7")

    def execute(self) -> None:
        """Optimizations:
        - Column projection pruning on all JDBC reads.
        - Broadcast joins on all small dimension tables to avoid shuffle stages.
        - cache() on the joined base DataFrame to avoid recomputing the 7-table pipeline.
        """
        rental_df = (
            self.load_table("rental")
            .where(col("return_date").isNotNull())
            .select("inventory_id", "customer_id", "rental_date", "return_date")
        )
        inventory_df = self.load_table("inventory").select("inventory_id", "film_id")
        film_category_df = self.load_table("film_category").select("film_id", "category_id")
        category_df = self.load_table("category").select("category_id", "name")
        customer_df = self.load_table("customer").select("customer_id", "address_id")
        address_df = self.load_table("address").select("address_id", "city_id")
        city_df = self.load_table("city").select("city_id", "city")

        cities_a_df = (
            city_df
            .where(col("city").rlike("(?i)^a"))
            .select(col("city_id"), col("city").alias("city_starts_with_a"))
        )
        cities_hyphen_df = (
            city_df
            .where(col("city").contains("-"))
            .select(col("city_id"), col("city").alias("city_with_hyphens"))
        )

        # city_starts_with_a | city_with_hyphens | city_id
        cities_df = cities_a_df.join(
            cities_hyphen_df, on="city_id", how="full"
        )

        # city_starts_with_a | city_with_hyphens | name | rental_hours
        rental_hours_df = (
            rental_df
            .withColumn(
                "rental_hours",
                (col("return_date").cast("long") - col("rental_date").cast("long")) / 3600,
            )
            .join(broadcast(inventory_df), on="inventory_id")
            .join(broadcast(film_category_df), on="film_id")
            .join(broadcast(category_df), on="category_id")
            .join(broadcast(customer_df), on="customer_id")
            .join(broadcast(address_df), on="address_id")
            .join(broadcast(cities_df), on="city_id")
            .select("city_starts_with_a", "city_with_hyphens", "name", "rental_hours")
        ).cache()

        ranking_window = Window.partitionBy(
            "city_starts_with_a", "city_with_hyphens"
        ).orderBy(desc("total_rental_hours"))

        result_df = (
            rental_hours_df
            .groupBy("city_starts_with_a", "city_with_hyphens", "name")
            .agg(sum("rental_hours").alias("total_rental_hours"))
            .withColumn("rank", dense_rank().over(ranking_window))
            .where(col("rank") == 1)
            .drop("rank")
            .orderBy("city_starts_with_a", "city_with_hyphens", desc("total_rental_hours"), "name")
        )

        self.json_inload(result_df)
        rental_hours_df.unpersist()
