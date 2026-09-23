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
        """Optimized:
        - Applied broadcast joins on all dimension tables ('inventory', 'film_category', 'category', 'customer', 'address', 'city') to eliminate shuffle stages against the large rental table.
        - Pruned intermediate columns down to only ('name', 'city', 'rental_hours') before downstream processing.
        - Cached the joined base DataFrame in memory to eliminate duplicate execution of the 7-table pipeline across multiple outputs, and unpersisted when finished.
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

        # Build joined DataFrame with broadcast dimension hints and cache to prevent re-computation
        rental_hours_df = (
            rental_df
            .withColumn("rental_hours", (col("return_date").cast("long") - col("rental_date").cast("long")) / 3600)
            .join(broadcast(inventory_df), on="inventory_id")
            .join(broadcast(film_category_df), on="film_id")
            .join(broadcast(category_df), on="category_id")
            .join(broadcast(customer_df), on="customer_id")
            .join(broadcast(address_df), on="address_id")
            .join(broadcast(city_df), on="city_id")
            .select("name", "city", "rental_hours")
        ).cache()

        ranking_window = Window.orderBy(desc("total_rental_hours"))

        def top_category_by_city_filter(df, filter_condition, label):
            result_df = (
                df
                .filter(filter_condition)
                .groupBy("name")
                .agg(sum("rental_hours").alias("total_rental_hours"))
                .withColumn("rank", dense_rank().over(ranking_window))
                .filter(col("rank") == 1)
                .drop("rank")
            )
            self.json_inload(result_df, path=f"practice/.results/task7/{label}")

        # Cities starting with letter "a" (case-insensitive)
        top_category_by_city_filter(
            rental_hours_df,
            col("city").startswith("a") | col("city").startswith("A"),
            label="cities_starting_with_a"
        )

        # Cities containing a "-" symbol
        top_category_by_city_filter(
            rental_hours_df,
            col("city").contains("-"),
            label="cities_with_dash"
        )

        rental_hours_df.unpersist()

