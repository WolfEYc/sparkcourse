import random
import re
import os

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, udtf, explode, lower
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import regexp_extract

LOG_FILE = "data/bluesky.ndjson"

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
    .option("rowsPerSecond", 50) \
    .load()
    
# feed DataFrame with canned log data
accessLines = rate_df \
    .withColumn("value", get_random_log_line())
    
hashtag_regex = re.compile(r'#(\w+)')

@udf("array<string>")
def extract_hashtags(line: str) -> list[str]:
    post = json.loads(line)
    text = post["text"]
    hashtags = re.findall(hashtag_regex, text)
    return hashtags


hashtags = accessLines.select(explode(extract_hashtags("value")).alias("hashtag"))
hashtags = hashtags.select(lower("hashtag").alias("hashtag"))

hashtags.createOrReplaceTempView("hashtags")

# Keep a running count of every access by status code
statusCountsDF = spark.sql("""
    SELECT hashtag, COUNT(*) as count
    FROM hashtags
    GROUP BY hashtag
    ORDER BY count DESC
    LIMIT 10
""")

# Kick off our streaming query, dumping results to the console
query = ( statusCountsDF.writeStream.outputMode("complete").format("console").option("truncate", "false").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()

