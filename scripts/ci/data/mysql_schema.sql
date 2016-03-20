USE airflow_ci;

CREATE TABLE IF NOT EXISTS baby_names (
  org_year integer(4),
  baby_name VARCHAR(25),
  rate FLOAT(7,6),
  sex VARCHAR(4)
)