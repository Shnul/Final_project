from elasticsearch import Elasticsearch, helpers

# Step 1: Verify the Elasticsearch server URL
es = Elasticsearch(['http://localhost:9200'])

# Step 2: Define the actions to be indexed
actions = [
    # Your actions here
]

# Step 3: Use the bulk helper to index the actions
try:
    helpers.bulk(es, actions)
except elastic_transport.ConnectionError as e:
    print(f"Connection error: {e}")