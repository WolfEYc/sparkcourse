from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  # ty:ignore[possibly-missing-attribute]

schema = StructType([ \
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True), 
    StructField("amt", FloatType(), True), 
])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("data/customer-orders.csv")
df.printSchema()


df.createOrReplaceTempView("transactions")

amt_spent_by_customer = spark.sql("""
SELECT user_id, ROUND(SUM(amt), 2) AS total_spend
FROM transactions
GROUP BY user_id        
ORDER BY sum(amt) DESC
""")
amt_spent_by_customer.show()
