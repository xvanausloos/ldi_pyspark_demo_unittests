import pytest
from google.api_core.retry.retry_unary_async import retry_target
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.testing import assertSchemaEqual

from app import pyspark_bike


@pytest.fixture
def spark_fixture() -> SparkSession:
    spark = SparkSession.builder.appName("Testing Bike calculation").getOrCreate()
    yield spark


@pytest.fixture
def source_fixture(spark_fixture) -> DataFrame:
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
    return spark_fixture.createDataFrame(data, schema)


def test_clean_data(spark_fixture: SparkSession, source_fixture: DataFrame):
    source_fixture.show(n=10)
    assert True


def test_schema(spark_fixture: SparkSession, source_fixture: DataFrame):
    b = pyspark_bike.Bike
    df_result = b.clean_data(df=source_fixture)

    assertSchemaEqual(
        df_result.schema,
        StructType(
            [
                StructField("loop_number", StringType()),
                StructField("label", StringType()),
                StructField("total", IntegerType()),
                StructField("date", DateType()),
                StructField("holiday_name", StringType()),
            ]
        ),
    )
