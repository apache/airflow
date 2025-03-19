import psycopg2

# Define connection parameters
DB_CONFIG = {
    "host": "database-1.c786gicssv1r.us-east-1.rds.amazonaws.com",
    "port": "5432",
    "user": "postgres",
    "password": "40QxPyn4ARlB3zOhwTD6",
    "dbname": "postgres"
}

# Establish connection
conn = psycopg2.connect(**DB_CONFIG)
print("Connected successfully!")
conn.close()
