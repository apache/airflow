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
Example DAG demonstrating MariaDB cpimport operations.

This DAG shows how to use the MariaDBCpImportOperator for high-performance
bulk data loading into ColumnStore tables.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.mariadb.operators.cpimport import MariaDBCpImportOperator
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
    "example_mariadb_cpimport",
    default_args=default_args,
    description="MariaDB cpimport operations example",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["example", "mariadb", "cpimport", "columnstore"],
)
# [END instantiate_dag]

# [START create_columnstore_table]
create_columnstore_table = MariaDBOperator(
    task_id="create_columnstore_table",
    mariadb_conn_id="mariadb_default",
    sql="""
    CREATE TABLE IF NOT EXISTS sales_data (
        id INT AUTO_INCREMENT,
        product_id INT,
        customer_id INT,
        sale_date DATE,
        quantity INT,
        unit_price DECIMAL(10,2),
        total_amount DECIMAL(12,2),
        region VARCHAR(50),
        sales_rep VARCHAR(100),
        PRIMARY KEY (id)
    ) ENGINE=ColumnStore
    """,
    dag=dag,
)
# [END create_columnstore_table]

# [START bulk_load_sales_data]
bulk_load_sales_data = MariaDBCpImportOperator(
    task_id="bulk_load_sales_data",
    table_name="sales_data",
    file_path="/var/data/sales_data_2024.csv",
    schema="analytics",
    mariadb_conn_id="mariadb_default",
    ssh_conn_id="ssh_default",
    cpimport_options={
        "-s": "','",           # Field separator
        "-E": '"',             # Field enclosure
        "-n": "\\N",           # NULL value representation
        "-m": "1",             # Mode (1=replace, 2=append)
        "-e": "1",             # Error handling
        "-w": "1000"           # Write buffer size
    },
    dag=dag,
)
# [END bulk_load_sales_data]

# [START create_product_table]
create_product_table = MariaDBOperator(
    task_id="create_product_table",
    mariadb_conn_id="mariadb_default",
    sql="""
    CREATE TABLE IF NOT EXISTS product_catalog (
        product_id INT,
        product_name VARCHAR(200),
        category VARCHAR(100),
        brand VARCHAR(100),
        price DECIMAL(10,2),
        in_stock BOOLEAN,
        created_date DATE,
        PRIMARY KEY (product_id)
    ) ENGINE=ColumnStore
    """,
    dag=dag,
)
# [END create_product_table]

# [START bulk_load_product_data]
bulk_load_product_data = MariaDBCpImportOperator(
    task_id="bulk_load_product_data",
    table_name="product_catalog",
    file_path="/var/data/product_catalog.csv",
    schema="analytics",
    mariadb_conn_id="mariadb_default",
    ssh_conn_id="ssh_default",
    cpimport_options={
        "-s": "','",
        "-E": '"',
        "-n": "\\N",
        "-m": "2",             # Append mode
        "-e": "1"
    },
    dag=dag,
)
# [END bulk_load_product_data]

# [START create_customer_table]
create_customer_table = MariaDBOperator(
    task_id="create_customer_table",
    mariadb_conn_id="mariadb_default",
    sql="""
    CREATE TABLE IF NOT EXISTS customer_info (
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
    ) ENGINE=ColumnStore
    """,
    dag=dag,
)
# [END create_customer_table]

# [START bulk_load_customer_data]
bulk_load_customer_data = MariaDBCpImportOperator(
    task_id="bulk_load_customer_data",
    table_name="customer_info",
    file_path="/var/data/customer_info.csv",
    schema="analytics",
    mariadb_conn_id="mariadb_default",
    ssh_conn_id="ssh_default",
    cpimport_options={
        "-s": "','",
        "-E": '"',
        "-n": "\\N",
        "-m": "2",
        "-e": "1"
    },
    dag=dag,
)
# [END bulk_load_customer_data]

# [START validate_data_loaded]
validate_data_loaded = MariaDBOperator(
    task_id="validate_data_loaded",
    mariadb_conn_id="mariadb_default",
    sql="""
    SELECT 
        'sales_data' as table_name,
        COUNT(*) as record_count,
        MIN(sale_date) as earliest_date,
        MAX(sale_date) as latest_date
    FROM analytics.sales_data
    UNION ALL
    SELECT 
        'product_catalog' as table_name,
        COUNT(*) as record_count,
        MIN(created_date) as earliest_date,
        MAX(created_date) as latest_date
    FROM analytics.product_catalog
    UNION ALL
    SELECT 
        'customer_info' as table_name,
        COUNT(*) as record_count,
        MIN(registration_date) as earliest_date,
        MAX(registration_date) as latest_date
    FROM analytics.customer_info
    """,
    dag=dag,
)
# [END validate_data_loaded]

# [START cleanup_tables]
cleanup_tables = MariaDBOperator(
    task_id="cleanup_tables",
    mariadb_conn_id="mariadb_default",
    sql="""
    DROP TABLE IF EXISTS analytics.sales_data;
    DROP TABLE IF EXISTS analytics.product_catalog;
    DROP TABLE IF EXISTS analytics.customer_info;
    """,
    dag=dag,
)
# [END cleanup_tables]

# [START task_dependencies]
# Create tables in parallel
[create_columnstore_table, create_product_table, create_customer_table] >> \
# Load data in parallel
[bulk_load_sales_data, bulk_load_product_data, bulk_load_customer_data] >> \
# Validate and cleanup
validate_data_loaded >> cleanup_tables
# [END task_dependencies]
