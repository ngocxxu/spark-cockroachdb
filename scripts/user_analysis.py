from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("UserAnalysis") \
    .master("local[*]") \
    .config("spark.jars", "./postgresql-42.7.4.jar") \
    .getOrCreate()

# CockroachDB connection string
db_url = os.getenv("DATABASE_URL_LOCAL_MYSQL")
jdbc_db_url = os.getenv("JDBC_DATABASE_URL_LOCAL_MYSQL")


# Read data from CockroachDB
user_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_db_url) \
    .option("dbtable", "user_data") \
    .option("user", "root") \
    .option("password", "") \
    .option("driver", "org.postgresql.Driver") \
    .load()
    

# Measure read data with 3/6 nodes
# # Start time
# start_time = time.time()
# # Read data from CockroachDB
# user_df = spark.read \
#     .format("jdbc") \
#     .option("url", jdbc_db_url) \
#     .option("dbtable", "user_data") \
#     .option("user", "root") \
#     .option("password", "") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()
# # End time
# end_time = time.time()
# elapsed_time = end_time - start_time
# print(f"Data read from CockroachDB in {elapsed_time:.2f} seconds with 6 nodes")


# Analyze top spenders
top_spenders = user_df.groupBy("user_name") \
                      .agg(F.sum("total_spend").alias("total_spend")) \
                      .orderBy("total_spend", ascending=False) \
                      .limit(100)  # Limit to top 100 spenders

top_spenders.show()

# Get list of top 100 spenders
top_spenders_list = [row['user_name'] for row in top_spenders.collect()]

# Analyze spend trend over time for top 100 spenders
user_df = user_df.withColumn("year_month", F.date_format("date", "yyyy-MM"))

spend_trend = user_df.filter(F.col("user_name").isin(top_spenders_list)) \
                     .groupBy("year_month") \
                     .agg(F.sum("total_spend").alias("total_spend")) \
                     .orderBy("year_month")

# Convert to pandas DataFrame for visualization
spend_trend_pandas = spend_trend.toPandas()

# Function to safely convert to datetime
def safe_to_datetime(date_string):
    try:
        return pd.to_datetime(date_string, format='%Y-%m')
    except Exception:
        # Return NaT (Not a Time) for invalid dates
        return pd.NaT

# Apply safe conversion and filter out invalid dates
spend_trend_pandas['year_month'] = spend_trend_pandas['year_month'].apply(safe_to_datetime)
spend_trend_pandas = spend_trend_pandas.dropna(subset=['year_month'])

# Sort the dataframe by year_month
spend_trend_pandas = spend_trend_pandas.sort_values('year_month')

# Plot spending trend over time
plt.figure(figsize=(15, 8))
plt.plot(spend_trend_pandas['year_month'], spend_trend_pandas['total_spend'], marker='o', markersize=5, linewidth=2)
plt.title("Monthly Spend Trend for 100 Users")
plt.xlabel("Month")
plt.ylabel("Total Spend")
plt.xticks(spend_trend_pandas['year_month'][::3], rotation=45, fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
plt.legend(['Total Spend'], loc='upper left')

# Format x-axis to show only year and month
plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m'))
plt.gca().xaxis.set_major_locator(plt.matplotlib.dates.MonthLocator(interval=300))

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