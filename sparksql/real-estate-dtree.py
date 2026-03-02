from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession, DataFrame, functions as func
from typing import cast

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("RealEstate-DTree").getOrCreate()

    # Load up our data and convert it to the format MLLib expects.
    df = spark.read \
        .option("header", "true").option("inferSchema", "true") \
        .csv("data/realestate.csv")
    # data = inputLines.map(lambda x: x.split(",")).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))
    vec_assembler = VectorAssembler(
        inputCols=[
            "HouseAge",
            "DistanceToMRT",
            "NumberConvenienceStores",
        ],
        outputCol="features"
    )
    df = vec_assembler.transform(df)
    df = df.select("PriceOfUnitArea", "features")

    # Let's split our data into training data and testing data
    trainTest = df.randomSplit([0.8, 0.2])
    trainingDF = trainTest[0]
    testDF = trainTest[1]

    # Now create our linear regression model
    lir = DecisionTreeRegressor().setLabelCol("PriceOfUnitArea").setFeaturesCol("features")

    # Train the model using our training data
    model = lir.fit(trainingDF)

    # Now see if we can predict values in our test data.
    # Generate predictions using our linear regression model for all features in our
    # test dataframe:
    fullPredictions = model.transform(testDF).cache()
    fullPredictions = cast(DataFrame, fullPredictions)
    fullPredictions.select("PriceOfUnitArea", func.round("prediction", 1).alias("prediction"), "features") \
        .show(n=fullPredictions.count())


    # Stop the session
    spark.stop()

