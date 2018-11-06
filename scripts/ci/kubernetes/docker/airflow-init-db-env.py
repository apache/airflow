from sqlalchemy import create_engine
print("Start postgres connnection")
engine = create_engine('postgresql://root:root@postgres-airflow/postgres')
conn = engine.connect()
conn.execute("commit")
user_exist = conn.execute("SELECT * FROM pg_catalog.pg_roles WHERE rolname = 'airflow'")
if not user_exist.fetchall():
    print("user 'airflow' not exist, create.")
    conn.execute("CREATE USER airflow PASSWORD 'airflow';")
    conn.execute("commit")
else:
    print("user 'airflow exist, skip.")

db_exist = conn.execute("SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('airflow')")
if not db_exist.fetchall():
    print("DB 'airflow' not exist, create.")
    conn.execute("CREATE DATABASE airflow;")
    conn.execute("commit")    
    conn.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;")
    conn.execute("commit")
else:
    print("DB 'airflow exist, skip.")
