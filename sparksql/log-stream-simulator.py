import random
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_extract

LOG_FILE = "data/access_log.txt"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SparkStreamingLogSimulator") \
    .getOrCreate()

if hasattr(spark, "sparkContext"):
    spark.sparkContext.setLogLevel("ERROR")

@udf(StringType())
def get_random_log_line():
    """Returns a random line from the log file without reading the entire file into memory."""
    try:
        if not os.path.exists(LOG_FILE):
            return None  # Handle the case where the log file is missing

        file_size = os.path.getsize(LOG_FILE)
        if file_size == 0:
            return None  # Handle empty file scenario

        with open(LOG_FILE, "r") as lf:
            while True:
                random_position = random.randint(0, file_size - 1)  # Pick a random position
                lf.seek(random_position)  # Jump to that position
                lf.readline()  # Discard partial line (move to next full line)
                line = lf.readline().strip()  # Read a full line
                
                if line:  # Ensure we get a valid line
                    return line

    except Exception as e:
        print(str(e))
        return None
    
# Create Streaming DataFrame from rate source
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \
    .load()
    
# feed DataFrame with canned log data
accessLines = rate_df \
    .withColumn("value", get_random_log_line())
    
# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# Keep a running count of every access by status code
statusCountsDF = logsDF.groupBy(logsDF.status).count()

# Kick off our streaming query, dumping results to the console
query = ( statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()
