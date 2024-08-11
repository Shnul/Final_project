import pandas as pd
from elasticsearch import Elasticsearch, helpers
import elastic_transport

# Step 1: Read the CSV file
csv_file_path = r'C:\tmp\Shahar\Final_Project\ELK\crypto_dummy_data.csv'
df = pd.read_csv(csv_file_path)

# Step 2: Create an Elasticsearch client
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Step 3: Prepare data for indexing
def generate_actions(dataframe, index_name):
    for _, row in dataframe.iterrows():
        yield {
            "_index": index_name,
            "_source": row.to_dict(),
        }

# Step 4: Index data into Elasticsearch
index_name = 'crypto_api'
actions = generate_actions(df, index_name)

try:
    helpers.bulk(es, actions)
    print(f"Data indexed successfully into {index_name}")
except elastic_transport.ConnectionError as e:
    print(f"Connection error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")