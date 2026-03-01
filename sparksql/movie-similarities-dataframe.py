from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computeCosineSimilarity(spark, data: DataFrame) -> DataFrame:
  
    # Compute xx, xy and yy columns
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2")) 

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores \
      .groupBy("movie1", "movie2") \
      .agg( \
        func.sum(func.col("xy")).alias("numerator"), \
        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
        func.count(func.col("xy")).alias("numPairs"),
        func.avg("rating1").alias("rating1"),
        func.avg("rating2").alias("rating2")
      )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    result = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs", "rating1", "rating2")

    return result

# Get movie name by given movie id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId) \
        .select("movieTitle").collect()[0]

    return result[0]


spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # ty:ignore[possibly-missing-attribute]

movieNamesSchema = StructType([ \
                               StructField("movieID", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True) \
                               ])
    
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
    
# Create a broadcast dataset of movieID and movieTitle.
# Apply ISO-885901 charset
movieNames = spark.read \
      .option("sep", "|") \
      .option("charset", "ISO-8859-1") \
      .schema(movieNamesSchema) \
      .csv("data/ml-100k/u.item")

# Load up movie data as dataset
movies = spark.read \
      .option("sep", "\t") \
      .schema(moviesSchema) \
      .csv("data/ml-100k/u.data")


ratings = movies.select("userId", "movieId", "rating").filter(func.col("rating") >= 4)

# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
moviePairs = ratings.alias("ratings1") \
      .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
      .select(func.col("ratings1.movieId").alias("movie1"), \
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.rating").alias("rating1"), \
        func.col("ratings2.rating").alias("rating2"))


moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))
    with_rating = filteredResults.withColumn("rating", func.when(func.col("movie1") == movieID, func.col("rating2")).otherwise(func.col("rating1")))
    with_rank = with_rating.withColumn("rank", (func.col("rating") / 5) * func.col("score") * func.col("numPairs"))
    top_10 = with_rank.sort(func.col("rank").desc()).limit(10)

    # Sort by quality score.
    
    print ("Top 10 similar movies for " + getMovieName(movieNames, movieID))
    
    results = top_10.select(
        func.when(func.col("movie1") == movieID, func.col("movie2")).otherwise(func.col("movie1")).alias("movieID"),
        "score",
        "rating",
        "numPairs",
        "rank",
    )
    results = results.join(movieNames, on="movieID")
    results = results.sort(func.col("rank").desc())
    results.show()
        
