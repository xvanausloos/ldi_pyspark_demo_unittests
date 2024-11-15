from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.connect.client.core import logger
from common_ldi.ldi_logger import LdiLogger

def main() -> None:
  b = Bike()
  source_file = "data/244400404_comptages-velo-nantes-metropole.csv"
  df = b._read_data(source_file)
  df.printSchema()
  df.show(n=100)


class Bike:
    def __init__(self) -> None:
        self._logger = LdiLogger.getlogger("ldi_python")
        self._spark = SparkSession.builder.appName("Bike calculation").getOrCreate()


    def _read_data(self, file: str) -> DataFrame:
        logger.info("Reading data {file}")
        df = (self._spark.read.format("csv")
              .option("delimiter", ";")
              .option("header", True)
              .load(file))
        return df

if __name__ == '__main__':
    main()