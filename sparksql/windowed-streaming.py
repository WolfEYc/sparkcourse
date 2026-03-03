# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 09:15:05 2019

@author: Frank
"""

from pyspark.sql import SparkSession, functions as func

from pyspark.sql.functions import regexp_extract

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("StructuredStreaming").getOrCreate()

if hasattr(spark, "sparkContext"):
    spark.sparkContext.setLogLevel("ERROR")
    

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
accessLines = spark.readStream.text("data/logs")

# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(
    func.current_timestamp().alias("event_time"),
    regexp_extract('value', hostExp, 1).alias('host'),
    regexp_extract('value', timeExp, 1).alias('timestamp'),
    regexp_extract('value', generalExp, 1).alias('method'),
    regexp_extract('value', generalExp, 2).alias('endpoint'),
    regexp_extract('value', generalExp, 3).alias('protocol'),
    regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
    regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'),
)

# Keep a running count of every access by status code
url_counts_df = logsDF.groupBy(func.window(func.col("event_time"), windowDuration="30 seconds", slideDuration="10 seconds"), func.col("endpoint")).count()
url_counts_df = url_counts_df.orderBy(func.col("count").desc())

# Kick off our streaming query, dumping results to the console
query = ( url_counts_df.writeStream.outputMode("complete").format("console").option("truncate", "false").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()


