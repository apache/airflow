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
Example DAG demonstrating MariaDB S3 integration.

This DAG shows how to use the MariaDB S3 operators for loading data from S3
to MariaDB and exporting data from MariaDB to S3.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.mariadb.operators.mariadb import MariaDBOperator
from airflow.providers.mariadb.operators.s3 import MariaDBS3LoadOperator, MariaDBS3DumpOperator

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
    "example_mariadb_s3",
    default_args=default_args,
    description="MariaDB S3 integration example",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["example", "mariadb", "s3", "aws"],
)
# [END instantiate_dag]

# [START create_tables]
create_tables = MariaDBOperator(
    task_id="create_tables",
    mariadb_conn_id="mariadb_default",
    sql="""
    -- Create source table for data loading
    CREATE TABLE IF NOT EXISTS raw_customer_data (
        customer_id INT,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(255),
        phone VARCHAR(20),
        address VARCHAR(500),
        city VARCHAR(100),
        state VARCHAR(50),
        zip_code VARCHAR(10),
        country VARCHAR(50),
        registration_date DATE,
        PRIMARY KEY (customer_id)
    ) ENGINE=InnoDB;
    
    -- Create processed table for data export
    CREATE TABLE IF NOT EXISTS processed_customer_data (
        customer_id INT,
        full_name VARCHAR(200),
        email VARCHAR(255),
        location VARCHAR(200),
        registration_date DATE,
        customer_segment VARCHAR(50),
        PRIMARY KEY (customer_id)
    ) ENGINE=InnoDB;
    """,
    dag=dag,
)
# [END create_tables]

# [START load_customer_data_from_s3]
load_customer_data_from_s3 = MariaDBS3LoadOperator(
    task_id="load_customer_data_from_s3",
    s3_bucket="my-data-lake",
    s3_key="raw-data/customers/{{ ds }}/customer_data.csv",
    table_name="raw_customer_data",
    schema="analytics",
    mariadb_conn_id="mariadb_default",
    aws_conn_id="aws_default",
    ssh_conn_id="ssh_default",
    dag=dag,
)
# [END load_customer_data_from_s3]

# [START process_customer_data]
process_customer_data = MariaDBOperator(
    task_id="process_customer_data",
    mariadb_conn_id="mariadb_default",
    sql="""
    INSERT INTO analytics.processed_customer_data (
        customer_id,
        full_name,
        email,
        location,
        registration_date,
        customer_segment
    )
    SELECT 
        customer_id,
        CONCAT(first_name, ' ', last_name) as full_name,
        email,
        CONCAT(city, ', ', state, ' ', zip_code) as location,
        registration_date,
        CASE 
            WHEN registration_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) THEN 'New'
            WHEN registration_date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY) THEN 'Active'
            ELSE 'Legacy'
        END as customer_segment
    FROM analytics.raw_customer_data
    WHERE email IS NOT NULL 
    AND email != ''
    """,
    dag=dag,
)
# [END process_customer_data]

# [START export_processed_data_to_s3_csv]
export_processed_data_to_s3_csv = MariaDBS3DumpOperator(
    task_id="export_processed_data_to_s3_csv",
    table_name="processed_customer_data",
    s3_bucket="my-data-warehouse",
    s3_key="processed-data/customers/{{ ds }}/processed_customers.csv",
    schema="analytics",
    mariadb_conn_id="mariadb_default",
    aws_conn_id="aws_default",
    ssh_conn_id="ssh_default",
    file_format="csv",
    dag=dag,
)
# [END export_processed_data_to_s3_csv]

# [START export_customer_segments_to_s3_json]
export_customer_segments_to_s3_json = MariaDBS3DumpOperator(
    task_id="export_customer_segments_to_s3_json",
    table_name="processed_customer_data",
    s3_bucket="my-data-warehouse",
    s3_key="analytics/customer-segments/{{ ds }}/customer_segments.json",
    query="""
    SELECT 
        customer_segment,
        COUNT(*) as customer_count,
        MIN(registration_date) as earliest_registration,
        MAX(registration_date) as latest_registration
    FROM analytics.processed_customer_data
    GROUP BY customer_segment
    ORDER BY customer_count DESC
    """,
    mariadb_conn_id="mariadb_default",
    aws_conn_id="aws_default",
    ssh_conn_id="ssh_default",
    file_format="json",
    dag=dag,
)
# [END export_customer_segments_to_s3_json]

# [START export_full_dataset_to_s3_sql]
export_full_dataset_to_s3_sql = MariaDBS3DumpOperator(
    task_id="export_full_dataset_to_s3_sql",
    table_name="processed_customer_data",
    s3_bucket="my-backup-bucket",
    s3_key="backups/customer-data/{{ ds }}/processed_customer_data.sql",
    schema="analytics",
    mariadb_conn_id="mariadb_default",
    aws_conn_id="aws_default",
    ssh_conn_id="ssh_default",
    file_format="sql",
    dag=dag,
)
# [END export_full_dataset_to_s3_sql]

# [START validate_exports]
validate_exports = MariaDBOperator(
    task_id="validate_exports",
    mariadb_conn_id="mariadb_default",
    sql="""
    SELECT 
        'raw_customer_data' as table_name,
        COUNT(*) as record_count,
        MIN(registration_date) as earliest_date,
        MAX(registration_date) as latest_date
    FROM analytics.raw_customer_data
    UNION ALL
    SELECT 
        'processed_customer_data' as table_name,
        COUNT(*) as record_count,
        MIN(registration_date) as earliest_date,
        MAX(registration_date) as latest_date
    FROM analytics.processed_customer_data;
    """,
    dag=dag,
)
# [END validate_exports]

# [START cleanup_tables]
cleanup_tables = MariaDBOperator(
    task_id="cleanup_tables",
    mariadb_conn_id="mariadb_default",
    sql="""
    DROP TABLE IF EXISTS analytics.raw_customer_data;
    DROP TABLE IF EXISTS analytics.processed_customer_data;
    """,
    dag=dag,
)
# [END cleanup_tables]

# [START task_dependencies]
create_tables >> \
load_customer_data_from_s3 >> \
process_customer_data >> \
[export_processed_data_to_s3_csv, export_customer_segments_to_s3_json, export_full_dataset_to_s3_sql] >> \
validate_exports >> \
cleanup_tables
# [END task_dependencies]
