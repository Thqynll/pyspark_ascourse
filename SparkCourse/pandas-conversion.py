from pyspark.sql import SparkSession
# Must set this env variable to avoid warnings
import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import pandas as pd
import pyspark.pandas as ps  # Alias for pandas API on Spark

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Pandas Integration with PySpark") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
    .getOrCreate()

# 1. Create a Pandas DataFrame
pandas_df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45]
})

print("Pandas DataFrame:")
print(pandas_df)

# 2. Convert Pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(pandas_df)

print("\nSchema of Spark DataFrame:")
spark_df.printSchema()

print("\nSpark DataFrame:")
spark_df.show()

# 3. Perform transformations on Spark DataFrame
filtered_spark_df = spark_df.filter(spark_df.age > 30)
print("\nFiltered Spark DataFrame (age > 30):")
filtered_spark_df.show()

# 4. Convert Spark DataFrame back to Pandas DataFrame
converted_pandas_df = filtered_spark_df.toPandas()
print("\nConverted Pandas DataFrame:")
print(converted_pandas_df)

# 5. Use pandas-on-Spark for scalable Pandas operations
ps_df = ps.DataFrame(pandas_df)

# Perform a Pandas-like operation in Spark
print("\nUsing pandas-on-Spark (incrementing age by 1):")
ps_df["age"] = ps_df["age"] + 1
print(ps_df)

# 6. Convert pandas-on-Spark DataFrame to Spark DataFrame
converted_spark_df = ps_df.to_spark()
print("\nConverted Spark DataFrame from pandas-on-Spark:")
converted_spark_df.show()

# Stop Spark session
spark.stop()
