from pyspark.sql.functions import col, count, desc, when
from general_cls import SparkTask


class SparkTask6(SparkTask):
    def __init__(self) -> None:
        super().__init__("task6")

    def execute(self) -> None:
        address_df = self.load_table("address").select("address_id", "city_id")
        city_df = self.load_table("city").select("city_id", "city")
        customer_df = self.load_table("customer").select("address_id", "active")

        result_df = (
            customer_df
            .join(address_df, on="address_id")
            .join(city_df, on="city_id")
            .groupBy("city")
            .agg(
                count(when(col("active") == 1, 1)).alias("active_customers"),
                count(when(col("active") != 1, 1)).alias("inactive_customers"),
            )
            .orderBy(desc("inactive_customers"), col("city"))
        )
        self.json_inload(result_df)
