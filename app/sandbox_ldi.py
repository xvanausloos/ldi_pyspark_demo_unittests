from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

if __name__ == '__main__':

    spark = SparkSession.builder.appName("test").getOrCreate()

    data = [
            (
                "0674", "Pont Haudaudine vers Sud", "657", "", "2", "0674 - Pont Haudaudine vers Sud", "2021-03-16",
                "Hors Vacances"
            ),
            (
                "0676", "Pont Willy Brandt vers Beaulieu", "480", "Faible", "1",
                "0676 - Pont Willy Brandt vers Beaulieu", "2021-05-31", "Hors Vacances"
            ),
        ]

    # Define the schema
    schema = StructType([
                StructField("boucle_num", StringType(), True),
                StructField("boucle_libelle", StringType(), True),
                StructField("total", StringType(), True),
                StructField("probabilite_presence_anomalie", StringType(), True),
                StructField("jour_de_la_semaine", StringType(), True),
                StructField("Boucle de comptage", StringType(), True),
                StructField("dateformat", StringType(), True),
                StructField("vacances_zone_b", StringType(), True),
        ])

    df = spark.createDataFrame(data=data, schema=schema)
    #df.show()
    print("End")
