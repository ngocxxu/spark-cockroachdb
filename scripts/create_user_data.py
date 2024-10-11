import pandas as pd # type: ignore
from faker import Faker # type: ignore
from datetime import datetime, timedelta

fake = Faker()

# Create a DataFrame to store user data
user_data = pd.DataFrame(columns=['user_id', 'user_name', 'email', 'total_spend', 'date', 'age', 'gender', 'location'])

# Create 100 sample users
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

# Save file CSV
user_data.to_csv('data/raw/user_data.csv', index=False)