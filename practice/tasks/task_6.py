from pyspark.sql.functions import col, count, desc, when, broadcast
from general_cls import SparkTask


class SparkTask6(SparkTask):
    """
    Output cities with the number of active and inactive customers (active - customer.active = 1). Sort by the number of inactive customers in descending order.
    """

    def __init__(self) -> None:
        super().__init__("task6")

    def execute(self) -> None:
        """Optimized:
        - Used broadcast joins on dimension tables 'address' (~1k rows) and 'city' (600 rows) to eliminate join shuffle stages.
        - Maintained selective column projection to avoid pulling unneeded attributes over JDBC.
        """
        address_df = self.load_table("address").select("address_id", "city_id")
        city_df = self.load_table("city").select("city_id", "city")
        customer_df = self.load_table("customer").select("address_id", "active")

        result_df = (
            customer_df
            .join(broadcast(address_df), on="address_id")
            .join(broadcast(city_df), on="city_id")
            .groupBy("city")
            .agg(
                count(when(col("active") == 1, 1)).alias("active_customers"),
                count(when(col("active") != 1, 1)).alias("inactive_customers"),
            )
            .orderBy(desc("inactive_customers"), col("city"))
        )
        self.json_inload(result_df)
