from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomerSpent").master("local[*]").getOrCreate()

schema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("item_id", StringType(), True),
    StructField("amount_spent", FloatType(), True)
])

spent = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")

one_spent = spent.groupBy("cust_id").agg(func.round(func.sum("amount_spent"), 2).alias("total_spent"))

one_spent_sorted = one_spent.sort("total_spent")

one_spent_sorted.show(one_spent_sorted.count())

spark.stop()