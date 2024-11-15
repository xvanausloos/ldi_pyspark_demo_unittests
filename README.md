# LDI Demo project for PySpark and Unit Tests
Created on 14/11/2024

Resources:
https://medium.com/plumbersofdatascience/introduction-to-creating-unit-tests-for-pyspark-applications-using-unittest-and-pytest-libraries-1fcc0feec945
https://garybake.com/pyspark_pytest.html
https://blog.eleven-labs.com/fr/demarrer-apache-spark/

## Prerequisite
Java 11 or Java 8

## Enable venv
`python -m venv .venv`
`source .venv\bin\activate`

## Run Python example 
From root folder: `python -m app.pyspark_bike`

## Run test 
From root folder: 

Run only tests with annotation `is_spark`: `pytest -s -m is_spark tests/`
All tests: `pytest tests`

    
## Read Parquet file 
Using 
```
pip install parquet-cli
parq <file name>.parquet --head 10
``` 