from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  # ty:ignore[possibly-missing-attribute]
schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("data/1800.csv")
df.printSchema()

# Filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# Select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
minTempsByStation = stationTemps.groupBy("stationID").agg(func.min("temperature").alias("min_temp"))
minTempsByStation.show()

# Convert temperature to fahrenheit and sort the dataset
minTempsByStationF = minTempsByStation.withColumn("min_temp", func.round(func.col("min_temp") * 0.1 * (9.0 / 5.0) + 32.0, 2))
minTempsByStationF = minTempsByStationF.select("stationID", "min_temp").sort("min_temp")
                                                  
# Collect, format, and print the results
results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
    
spark.stop()

                                                  
