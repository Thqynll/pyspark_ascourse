from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel+Names")

lines = spark.read.text("file:///SparkCourse/Marvel+Graph")

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
leastPopularCount = connections.agg(func.min("connections")).first()[0]

leastPopulars = connections.filter(func.col("connections") == leastPopularCount)

leastPopularwithNames = leastPopulars.join(names, "id")

print("The following characters have only " + str(leastPopularCount) + " connection(s):")

leastPopularwithNames.select("name").show()