from pyspark.sql import SparkSession # type: ignore
import pyspark.sql.functions as F # type: ignore
import matplotlib.pyplot as plt # type: ignore

# Create SparkSession
spark = SparkSession.builder.appName("UserAnalysis").master("local[*]").getOrCreate()

# Read data of users
user_df = spark.read.csv("data/raw/user_data.csv", header=True, inferSchema=True)

# Analyze top users by total spend
top_spenders = user_df.groupBy("user_name").\
                       agg(F.sum("total_spend").alias("total_spend")).\
                       orderBy("total_spend", ascending=False).\
                       limit(10)
top_spenders.show()

# Analyze user spend trends over time
user_df = user_df.withColumn("date", F.to_date("date"))
spend_trend = user_df.groupBy("date", "user_name").\
                     agg(F.sum("total_spend").alias("total_spend")).\
                     orderBy("date")

spend_trend_pandas = spend_trend.toPandas()
plt.figure(figsize=(12, 6))
spend_trend_pandas.plot(x="date", y="total_spend", kind="line")
plt.title("User Spend Trend")
plt.xlabel("Date")
plt.ylabel("Total Spend")
plt.show()

# Save DataFrame top_spenders into file CSV
top_spenders_df = top_spenders.toPandas()
top_spenders_df.to_csv("visualizations/top_spenders.csv", index=False)

# Save spend trend chart to image file
plt.savefig("visualizations/spend_trend.png")
