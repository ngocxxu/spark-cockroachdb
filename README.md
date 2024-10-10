# Spark + CockroachDB project

## Setup WSL for windows

## Access to Spark Master

- Open Visual Studio Code and press `Ctrl + Shift + P`
- Type and click `Dev Containers: Attach to Running Container...`
  ![alt text](image-1.png)

- Choose `/spark-cockroachdb-spark-master-1`
  ![alt text](image-2.png)

## Open terminal in Spark Master - Remote VS Code

- After complete `Access to Spark Master` step, press [Ctrl + `] to open terminal

- Type `pip install pyspark` to install pyspark
  ![alt text](image-3.png)

- Run `pyspark` to start Spark Shell

## Additional in Spark Master - Remote VS Code

- Create a structure folder

```
spark-project/
├── data/
│   ├── raw/
│   └── processed/
├── notebooks/
├── scripts/
│   ├── product_analysis.py
│   └── utils.py
├── visualizations/

```

- Relationship

```
product_id: Mã sản phẩm
product_name: Tên sản phẩm
category: Danh mục sản phẩm
price: Giá bán
quantity_sold: Số lượng bán
```

- `product_analysis.py`

```
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

# Create SparkSession
spark = SparkSession.builder.appName("ProductAnalysis").getOrCreate()

# Read data of products
product_df = spark.read.csv("data/raw/product_data.csv", header=True, inferSchema=True)

# Analyze top selling product
top_products = product_df.groupBy("product_name").\
                         agg(F.sum("quantity_sold").alias("total_sold")).\
                         orderBy("total_sold", ascending=False).\
                         limit(10)
top_products.show()

# Analyze high margin product (biên lợi nhuận)
product_df = product_df.withColumn("profit_margin", product_df.price - product_df.cost)
top_profit_products = product_df.orderBy("profit_margin", ascending=False).limit(10)
top_profit_products.show()

# Analyze sales trends over time
product_df = product_df.withColumn("date", F.to_date("date"))
sales_trend = product_df.groupBy("date", "product_name").\
                        agg(F.sum("quantity_sold").alias("total_sold")).\
                        orderBy("date")

sales_trend_pandas = sales_trend.toPandas()
plt.figure(figsize=(12, 6))
sales_trend_pandas.plot(x="date", y="total_sold", kind="line")
plt.title("Sales Trend by Product")
plt.xlabel("Date")
plt.ylabel("Total Sold")
plt.show()

# Save DataFrame top_products into file CSV
top_products_df = top_products.toPandas()
top_products_df.to_csv("visualizations/top_selling_products.csv", index=False)

# Save DataFrame top_profit_products into file CSV
top_profit_products_df = top_profit_products.toPandas()
top_profit_products_df.to_csv("visualizations/high_margin_products.csv", index=False)

# Save sales trend chart to image file
plt.savefig("visualizations/sales_trend.png")
```

- `create_product_data.py`

```
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Create a DataFrame to store product data
product_data = pd.DataFrame(columns=['product_id', 'product_name', 'category', 'price', 'quantity_sold', 'cost', 'date'])

# Create 100 sample products
product_list = []
start_date = datetime(2022, 1, 1)
for i in range(100):
    product_list.append({
        'product_id': f'P{i:03}',
        'product_name': fake.unique.word(),
        'category': fake.random_element(['Electronics', 'Clothing', 'Home', 'Sports', 'Books']),
        'price': round(fake.pyfloat(min_value=10, max_value=500, right_digits=2), 2),
        'quantity_sold': fake.random_int(min=50, max=1000),
        'cost': round(fake.pyfloat(min_value=5, max_value=300, right_digits=2), 2),
        'date': start_date + timedelta(days=i)
    })

product_data = pd.concat([product_data, pd.DataFrame(product_list)], ignore_index=True)

# Save file CSV
product_data.to_csv('data/raw/product_data.csv', index=False)
```

- Install `pip install matplotlib`
- Install `pip install faker pandas`
- Run `python3 scripts/create_product_data.py` to create 100 sample products
- Run `python3 scripts/product_analysis.py` to analyze top selling product, high margin product, sales trends over time
- ``
