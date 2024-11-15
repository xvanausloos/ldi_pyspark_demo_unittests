from pyspark.sql import SparkSession, DataFrame
from common_ldi.ldi_logger import LdiLogger
from pathlib import Path, PosixPath


def main() -> None:
    b = Bike()

    # Define the base directory relative to the current script file
    BASE_DIR = Path(__file__).resolve().parent.parent
    # Define the file path relative to BASE_DIR
    source_file = BASE_DIR / 'data' / '244400404_comptages-velo-nantes-metropole.csv'
    df = b.read_data(source_file)
    df.printSchema()
    df.show(n=100)


class Bike:
    def __init__(self) -> None:
        self._logger = LdiLogger.getlogger("ldi_python")
        self._spark = SparkSession.builder.appName("Bike calculation").getOrCreate()

    def read_data(self, file: PosixPath) -> DataFrame:
        df = (
            self._spark.read.format("csv")
            .option("delimiter", ";")
            .option("header", True)
            .load(str(file))
        )
        return df


if __name__ == "__main__":
    main()
