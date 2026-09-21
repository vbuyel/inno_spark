import logging
from tasks import (
    SparkTask1,
    SparkTask2,
    SparkTask3,
    SparkTask4,
    SparkTask5,
    SparkTask6,
    SparkTask7,
)

# Configure logging once at the application entrypoint
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting Spark tasks execution...")
    tasks = [
        SparkTask1(),
        SparkTask2(),
        SparkTask3(),
        SparkTask4(),
        SparkTask5(),
        SparkTask6(),
        SparkTask7(),
    ]

    try:
        for task in tasks:
            task.execute()
        logger.info("All tasks completed successfully.")
    except Exception:
        logger.exception("An error occurred during task execution:")
    finally:
        for task in tasks:
            task.close_all()
