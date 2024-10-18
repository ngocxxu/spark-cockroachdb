from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("UserAnalysis") \
    .master("local[*]") \
    .config("spark.jars", "./postgresql-42.7.4.jar") \
    .getOrCreate()

# CockroachDB connection string
db_url = os.getenv("DATABASE_URL_LOCAL")
jdbc_db_url = os.getenv("JDBC_DATABASE_URL_LOCAL")

# Read data from CockroachDB
user_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_db_url) \
    .option("dbtable", "user_data") \
    .option("user", "root") \
    .option("password", "") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Analyze top spenders
top_spenders = user_df.groupBy("user_name") \
                      .agg(F.sum("total_spend").alias("total_spend")) \
                      .orderBy("total_spend", ascending=False) \
                      .limit(100)  # Limit to top 100 spenders

top_spenders.show()

# Analyze spend trend over time
user_df = user_df.withColumn("date", F.to_date("date"))

spend_trend = user_df \
                    .groupBy("date") \
                    .agg(F.sum("total_spend").alias("total_spend")) \
                    .orderBy("date")

# Convert to pandas DataFrame for visualization
spend_trend_pandas = spend_trend.toPandas()

# Plot spending trend over time
plt.figure(figsize=(12, 6))
plt.plot(spend_trend_pandas['date'], spend_trend_pandas['total_spend'], marker='o')
plt.title("User Spend Trend")
plt.xlabel("Date")
plt.ylabel("Total Spend")
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.tight_layout()  # Adjust layout to prevent clipping of labels
plt.savefig("visualizations/spend_trend.png")
plt.show()


try:
    engine = create_engine(db_url)
    top_spenders_df = top_spenders.toPandas()
    
    # Save top spenders DataFrame to a CSV file
    top_spenders_df.to_csv("visualizations/top_spenders.csv", index=False)
    
    # Save results to CockroachDB
    top_spenders_df.to_sql("top_spenders", con=engine, if_exists="replace", index=False)
    spend_trend_pandas.to_sql("spend_trend", con=engine, if_exists="replace", index=False)

    print("Data successfully saved to CockroachDB")
except Exception as e:
    print(f"An error occurred while saving data: {e}")