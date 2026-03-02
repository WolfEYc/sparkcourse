from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession, types, DataFrame
from typing import cast

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("LinearRegression").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # ty:ignore[possibly-missing-attribute]

    schema = types.StructType([
        types.StructField("label", types.FloatType(), False),
        types.StructField("feature", types.FloatType(), False),
    ])
    # Load up our data and convert it to the format MLLib expects.
    df = spark.read.schema(schema).csv("data/regression.txt")
    # data = inputLines.map(lambda x: x.split(",")).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))
    vec_assembler = VectorAssembler(
        inputCols=["feature"],
        outputCol="features"
    )
    df = vec_assembler.transform(df)
    df = df.select("label", "features")
    # Convert this RDD to a DataFrame
    # colNames = ["label", "features"]
    # df = data.toDF(colNames)

    # Note, there are lots of cases where you can avoid going from an RDD to a DataFrame.
    # Perhaps you're importing data from a real database. Or you are using structured streaming
    # to get your data.

    # Let's split our data into training data and testing data
    trainTest = df.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Now create our linear regression model
    lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Train the model using our training data
    model = lir.fit(trainingDF)

    # Now see if we can predict values in our test data.
    # Generate predictions using our linear regression model for all features in our
    # test dataframe:
    fullPredictions = model.transform(testDF).cache()
    fullPredictions = cast(DataFrame, fullPredictions)
    fullPredictions.show(n=fullPredictions.count())


    # Stop the session
    spark.stop()

