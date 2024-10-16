from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import os  
from dotenv import load_dotenv  

load_dotenv()

sql_user_password = os.getenv('SQL_USER_PASSWORD')

# Create SparkSession
spark = SparkSession.builder \
    .appName("UserAnalysis") \
    .config("spark.driver.extraClassPath", "path/to/cockroachdb-jdbc.jar") \
    .getOrCreate()

# Connect to CockroachDB
jdbc_url = "postgresql://ngocxxu:{}@bono-projects-3828.j77.aws-ap-southeast-1.cockroachlabs.cloud:26257/spark_cockroach_db?sslmode=verify-full".format(sql_user_password)

# Read data of users from CockroachDB
user_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "user_data") \
    .option("user", "ngocxxu") \
    .option("password", sql_user_password) \
    .load()

# Analyze top users by total spend
top_spenders = user_df.groupBy("user_name") \
                      .agg(F.sum("total_spend").alias("total_spend")) \
                      .orderBy("total_spend", ascending=False) \
                      .limit(10)
top_spenders.show()

# Analyze user spend trends over time
user_df = user_df.withColumn("date", F.to_date("date"))
spend_trend = user_df \
                  .groupBy("date", "user_name") \
                  .agg(F.sum("total_spend").alias("total_spend")) \
                  .orderBy("date")

spend_trend_pandas = spend_trend.toPandas()
plt.figure(figsize=(12, 6))
spend_trend_pandas.plot(x="date", y="total_spend", kind="line")
plt.title("User Spend Trend")
plt.xlabel("Date")
plt.ylabel("Total Spend")
plt.show()

# Save DataFrame top_spenders into CockroachDB
top_spenders.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "top_spenders") \
    .option("user", "ngocxxu") \
    .option("password", sql_user_password) \
    .mode("overwrite") \
    .save()

# Save spend trend chart to image file
plt.savefig("visualizations/spend_trend.png")
