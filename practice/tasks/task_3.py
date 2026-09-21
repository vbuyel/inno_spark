from pyspark.sql import SparkSession
from db_connection import PostgresConnector


def create_spark_session():
    return SparkSession.builder.appName("Task3").getOrCreate()

def main():
    conn = PostgresConnector()
    db_conn = conn.connect()
    spark = create_spark_session()

    # Spark code
    
    spark.stop()
    db_conn.close()
    conn.disconnect()

if __name__ == "__main__":
    main()
