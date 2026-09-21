from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession


class SparkTask(ABC):
    @abstractmethod
    def create_spark_session(self) -> SparkSession:
        """Create a SparkSession."""
        pass

    @abstractmethod
    def load_table(self, table_name: str) -> DataFrame:
        """Load a PostgreSQL table into a Spark DataFrame."""
        pass

    @abstractmethod
    def execute(self) -> None:
        """Execute the main logic of the task using Spark."""
        pass

    @abstractmethod
    def close_all(self) -> None:
        """Close SparkSession and database connection."""
        pass

