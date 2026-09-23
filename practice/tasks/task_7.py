from general_cls import SparkTask


class SparkTask7(SparkTask):
    """
    Output the category of movies that have the highest number of total rental hours in the cities (customer.address_id in this city), and that start with the letter “a”. Do the same for cities with a “-” symbol.
    """

    def __init__(self) -> None:
        super().__init__("task7")

    def execute(self) -> None:
        rental_df = self.load_table("rental").select("inventor_id", "rental_date", "return_date")
        inventory_df = self.load_table("inventory").select("inventory_id", "film_id")
        film_df = self.load_table("film").select("film_id", "category_id")
        category_df = self.load_table("category").select("category_id", "name")
