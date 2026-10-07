from pyspark.sql import SparkSession
#from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL_frbyages").getOrCreate()

line = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///SparkCourse/fakefriends-header.csv")

#Select only the "age" and "friends" columns
friendsByAge = line.select("age", "friends")

#From friendsByAge, group by "age" and compute the average number of friends for each age.
friendsByAge.groupBy("age").avg("friends").show()

friendsByAge.groupBy("age").avg("friends").sort("age").show() # Sorted

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show() # Format the results

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)\
    .alias("friends_avg")).sort("age").show()  #custom a new column name 

spark.stop()

