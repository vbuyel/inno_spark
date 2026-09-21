from tasks.task_1 import SparkTask1
# from tasks.task_2 import SparkTask2
# from tasks.task_3 import SparkTask3
# from tasks.task_4 import SparkTask4
# from tasks.task_5 import SparkTask5
# from tasks.task_6 import SparkTask6
# from tasks.task_7 import SparkTask7


if __name__ == "__main__":
    tasks = [
        SparkTask1(),
        # SparkTask2(),
        # SparkTask3(),
        # SparkTask4(),
        # SparkTask5(),
        # SparkTask6(),
        # SparkTask7()
    ]

    try:
        for task in tasks:
            task.execute()
    finally:
        for task in tasks:
            task.close_all()
