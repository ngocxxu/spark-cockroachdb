import pandas as pd
import io

# Data get from visualizations/top_spenders.csv
data = """user_name,total_spend
Joan Martin,4993.83
Jesse Ellis,4966.71
Stephen Little,4907.74
James Chen,4906.39
Kathryn Bailey,4882.6
Steven Bruce,4840.3
Eric Johnson,4748.15
David Mcclure,4737.19
Douglas Cox,4719.0
Alexis Jones,4714.41"""

df = pd.read_csv(io.StringIO(data), sep=',')
display(df)