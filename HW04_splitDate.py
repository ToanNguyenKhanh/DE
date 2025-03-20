from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col

spark = SparkSession.builder \
    .appName("DE_ETL") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

data = [("11/12/2025",), ("27/02.2014",), ("2023.01.09",), ("28-12-2005",), (("11-27-2005",))]
df = spark.createDataFrame(data, ["date"])

def transform(date_str: str) -> list[int]:
    for sep in ["/", ".", " "]:
        date_str = date_str.replace(sep, "-")
    list_date = date_str.split("-")
    result = []
    for v in list_date: # append day/month hoac month/day
        if len(v) == 2:
            result.append(int(v))
    for v in list_date: # append year
        if len(v) == 4:
            result.append(int(v))
    if result[1] > 12: # month > 12 thi doi cho cho 'day'
        tmp = result[0]
        result[0] = result[1]
        result[1] = tmp
    return result

transformUDF = udf(transform, ArrayType(StringType()))
df = df.withColumn("date_transform", transformUDF(col("date")))
df = df.withColumn("day", col("date_transform")[0])
df = df.withColumn("month", col("date_transform")[1])
df = df.withColumn("year", col("date_transform")[2])

df.select("date", "day", "month", "year").show()
