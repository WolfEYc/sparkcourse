from pyspark.sql.functions import count, avg, round
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  # ty:ignore[possibly-missing-attribute]
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("data/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

print("avg friends per age")
people.groupBy("age").agg(round(avg("friends")).alias("avg_friends")).sort("age").show()

spark.stop()

