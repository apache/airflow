# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example DAG demonstrating basic MariaDB operations.

This DAG shows how to use the MariaDB operator for basic SQL operations
including table creation, data insertion, and querying.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.mariadb.operators.mariadb import MariaDBOperator

# [START default_args]
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "example_mariadb_basic",
    default_args=default_args,
    description="Basic MariaDB operations example",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["example", "mariadb", "sql"],
)
# [END instantiate_dag]

# [START create_table]
create_table = MariaDBOperator(
    task_id="create_table",
    mariadb_conn_id="mariadb_default",
    sql="""
    CREATE TABLE IF NOT EXISTS test_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        age INT,
        email VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """,
    dag=dag,
)
# [END create_table]

# [START insert_data]
insert_data = MariaDBOperator(
    task_id="insert_data",
    mariadb_conn_id="mariadb_default",
    sql="""
    INSERT INTO test_table (name, age, email) VALUES 
    ('Alice Johnson', 28, 'alice.johnson@example.com'),
    ('Bob Smith', 35, 'bob.smith@example.com'),
    ('Charlie Brown', 42, 'charlie.brown@example.com'),
    ('Diana Prince', 29, 'diana.prince@example.com'),
    ('Eve Wilson', 31, 'eve.wilson@example.com')
    """,
    dag=dag,
)
# [END insert_data]

# [START query_data]
query_data = MariaDBOperator(
    task_id="query_data",
    mariadb_conn_id="mariadb_default",
    sql="""
    SELECT 
        name,
        age,
        email,
        created_at
    FROM test_table 
    WHERE age > 30
    ORDER BY age DESC
    """,
    dag=dag,
)
# [END query_data]

# [START update_data]
update_data = MariaDBOperator(
    task_id="update_data",
    mariadb_conn_id="mariadb_default",
    sql="""
    UPDATE test_table 
    SET email = CONCAT(SUBSTRING_INDEX(email, '@', 1), '@updated.com')
    WHERE age > 35
    """,
    dag=dag,
)
# [END update_data]

# [START create_index]
create_index = MariaDBOperator(
    task_id="create_index",
    mariadb_conn_id="mariadb_default",
    sql="""
    CREATE INDEX IF NOT EXISTS idx_test_table_age 
    ON test_table (age)
    """,
    dag=dag,
)
# [END create_index]

# [START cleanup]
cleanup = MariaDBOperator(
    task_id="cleanup",
    mariadb_conn_id="mariadb_default",
    sql="DROP TABLE IF EXISTS test_table",
    dag=dag,
)
# [END cleanup]

# [START task_dependencies]
create_table >> insert_data >> [query_data, create_index] >> update_data >> cleanup
# [END task_dependencies]
