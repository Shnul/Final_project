import psycopg2

# Connect to PostgreSQL using localhost
conn = psycopg2.connect(
    host="localhost",
    database="cryptodb",
    user="shahar",
    password="64478868"
)

cur = conn.cursor()

# Execute a query
cur.execute("SELECT * FROM crypto_data")

# Fetch and print the results
rows = cur.fetchall()
for row in rows:
    print(row)

# Close the cursor and connection
cur.close()
conn.close()