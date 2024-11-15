from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

if __name__ == '__main__':

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("DataFrameExample") \
        .getOrCreate()

    # Define the data
    # data = [
    #     (
    #         "0674", "Pont Haudaudine vers Sud", "657", "Faible", "2",
    #         "0674 - Pont Haudaudine vers Sud", "2021-03-16", "Hors Vacances"
    #     ),
    #     (
    #         "0676", "Pont Willy Brandt vers Beaulieu", "480", "Faible", "1",
    #         "0676 - Pont Willy Brandt vers Beaulieu", "2021-05-31", "Hors Vacances"
    #     ),
    # ]
    #
    # # Define the schema
    # schema = StructType([
    #     StructField("boucle_num", StringType(), True),
    #     StructField("boucle_libelle", StringType(), True),
    #     StructField("total", StringType(), True),
    #     StructField("probabilite_presence_anomalie", StringType(), True),
    #     StructField("jour_de_la_semaine", StringType(), True),
    #     StructField("boucle_de_comptage", StringType(), True),  # Removed space
    #     StructField("dateformat", StringType(), True),
    #     StructField("vacances_zone_b", StringType(), True),
    # ])



    data2 = [("James", "", "Smith", "36636", "M", 3000),
             ("Michael", "Rose", "", "40288", "M", 4000),
             ("Robert", "", "Williams", "42114", "M", 4000),
             ("Maria", "Anne", "Jones", "39192", "F", 4000),
             ("Jen", "Mary", "Brown", "", "F", -1)
             ]

    schema = StructType([StructField("firstname", StringType(), True), StructField("middlename", StringType(), True),
                         StructField("lastname", StringType(), True), StructField("id", StringType(), True),
                         StructField("gender", StringType(), True), StructField("salary", IntegerType(), True)])

    # Create the DataFrame
    df = spark.createDataFrame(data=data2, schema=schema)
    #
    # # Show the DataFrame
    df.show()
    #
    # # Print schema to verify types
    # df.printSchema()
    print("end")
