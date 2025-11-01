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
Example DAG for MariaDB provider demonstrating basic usage.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.mariadb.operators.mariadb import MariaDBOperator

# [START howto_operator_mariadb]
with DAG(
    dag_id="example_mariadb",
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule=timedelta(minutes=10),
    default_args={
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    catchup=False,
    tags=["example", "mariadb"],
) as dag:
    # [START howto_operator_mariadb]
    create_table = MariaDBOperator(
        task_id="create_table",
        mariadb_conn_id="mariadb_default",
        sql="""
        CREATE TABLE IF NOT EXISTS test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    insert_data = MariaDBOperator(
        task_id="insert_data",
        mariadb_conn_id="mariadb_default",
        sql="""
        INSERT INTO test_table (name) VALUES 
        ('test_record_1'),
        ('test_record_2'),
        ('test_record_3');
        """,
    )

    select_data = MariaDBOperator(
        task_id="select_data",
        mariadb_conn_id="mariadb_default",
        sql="SELECT * FROM test_table;",
    )

    create_table >> insert_data >> select_data
    # [END howto_operator_mariadb]
