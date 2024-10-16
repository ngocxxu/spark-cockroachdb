import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

sql_user_password = os.getenv('SQL_USER_PASSWORD')

# Connect to CockroachDB
jdbc_url = f"postgresql://ngocxxu:{sql_user_password}@bono-projects-3828.j77.aws-ap-southeast-1.cockroachlabs.cloud:26257/spark_cockroach_db?sslmode=verify-full"

fake = Faker()

# Create DataFrame to save data
user_data = pd.DataFrame(columns=['user_id', 'user_name', 'email', 'total_spend', 'date', 'age', 'gender', 'location'])

# Create 100 samples
user_list = []
start_date = datetime(2022, 1, 1)
for i in range(100):
    user_list.append({
        'user_id': f'U{i:03}',
        'user_name': fake.unique.name(),
        'email': fake.email(),
        'total_spend': round(fake.pyfloat(min_value=50, max_value=5000, right_digits=2), 2),
        'date': start_date + timedelta(days=i),
        'age': fake.random_int(min=18, max=65),
        'gender': fake.random_element(['Male', 'Female', 'Other']),
        'location': f"{fake.city()}, {fake.country_code()}"
    })

user_data = pd.concat([user_data, pd.DataFrame(user_list)], ignore_index=True)

# Save data into CockroachDB
engine = create_engine(jdbc_url)

# Record data into user_data table
user_data.to_sql('user_data', con=engine, if_exists='replace', index=False)

print("Data already saved in CockroachDB!")
