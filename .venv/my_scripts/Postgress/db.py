import psycopg2

# Define the database connection parameters
db_params = {
    "dbname": "cryptodb",
    "user": "shahar",
    "password": "64478868",
    "host": "localhost",
    "port": 5433
}

# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Execute a query to fetch data from the users table
cur.execute("SELECT * FROM users;")
rows = cur.fetchall()

# Print the results
for row in rows:
    print(row)

# Close the cursor and connection
cur.close()
conn.close()