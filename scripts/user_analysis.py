from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os

spark = SparkSession.builder \
    .appName("UserAnalysis") \
    .master("local[*]") \
    .config("spark.jars", "/postgresql-42.7.4.jar") \
    .getOrCreate()

# CockroachDB connection string
db_url = os.getenv("DATABASE_URL")

# Read CockroachDB
user_df = spark.read \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "user_data") \
    .option("user", "ngocxxu") \
    .option("password", os.getenv("COCKROACH_PASSWORD")) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Analyze top spenders
top_spenders = user_df.groupBy("user_name") \
                      .agg(F.sum("total_spend").alias("total_spend")) \
                      .orderBy("total_spend", ascending=False) \
                      .limit(10)
top_spenders.show()

# Analyze spend trend over time
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

try:
    engine = create_engine(db_url)
    
    # Plot spend trend
    top_spenders_df = top_spenders.toPandas()
    top_spenders_df.to_sql("top_spenders", con=engine, if_exists="replace", index=False)

    # Save results to CockroachDB
    spend_trend_pandas.to_sql("spend_trend", con=engine, if_exists="replace", index=False)

    print("Data successfully saved to CockroachDB")
except Exception as e:
    print(f"An error occurred while saving data: {e}")