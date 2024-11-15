from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.connect.client.core import logger
from pyspark.sql.types import IntegerType, DateType

from common_ldi.ldi_logger import LdiLogger
from pathlib import Path
from pyspark.sql import functions as F


def main() -> None:
    b = Bike()

    # Define the base directory relative to the current script file
    BASE_DIR = Path(__file__).resolve().parent.parent
    # Define the file path relative to BASE_DIR
    csv_source_file = BASE_DIR / 'data' / '244400404_comptages-velo-nantes-metropole.csv'
    json_source_file = BASE_DIR / 'data' / '244400404_comptages-velo-nantes-metropole.json'
    # df = b.read_data_csv(csv_source_file)
    df = b.read_data_json(json_source_file)
    df.printSchema()
    df_clean = b.clean_data(df=df)
    df_clean.show(n=100)
    b.store_parquet(df_clean)
    logger.info("** End of the script **")


class Bike:
    def __init__(self) -> None:
        self._logger = LdiLogger.getlogger("ldi_python")
        self._spark = SparkSession.builder.appName("Bike calculation").getOrCreate()

    def read_data_csv(self, file: Path) -> DataFrame:
        df = (
            self._spark.read.format("csv")
            .option("delimiter", ";")
            .option("header", True)
            .load(str(file))
        )
        return df

    def read_data_json(self, file: Path) -> DataFrame:
        df = self._spark.read.json(str(file))
        df.printSchema()
        df.show(n=10)
        return df

    def clean_data(self, df: DataFrame) -> DataFrame:
        df = (df.select(
        F.col("boucle_num").alias("loop_number"),
        F.col("boucle_libelle").alias("label"),
        F.col("total").cast(IntegerType()).alias("total"),
        F.col("dateformat").cast(DateType()).alias("date"),
        F.col("vacances_zone_b").alias("holiday_name"),
        )
        .where(F.col("probabilite_presence_anomalie").isNull()))
        return df

    def store_parquet(self, df_clean: DataFrame) -> None:
        df_clean.write.format("parquet").partitionBy("date").save("datalake/count-bike-nantes.parquet")


if __name__ == "__main__":
    main()
