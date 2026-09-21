from general_cls import SparkTask


class SparkTask2(SparkTask):
    def __init__(self) -> None:
        super().__init__("task2")

    def execute(self) -> None:
        pass
