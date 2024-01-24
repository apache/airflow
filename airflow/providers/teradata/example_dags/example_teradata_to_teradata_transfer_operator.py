#
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
Example Airflow DAG to show usage of teradata to teradata transfer operator

The transfer operator connects to source teradata server, runs query to fetch data from source
and inserts that data into destination teradata database server. It assumes tables already exists.
The example DAG below assumes Airflow Connection with connection id `teradata_default` already exists.
It creates sample my_users table at source and destination, sets up sample data at source and then
runs transfer operator to copy data to corresponding table on destination server.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from airflow import DAG
from airflow.models.baseoperator import chain

try:
    from airflow.providers.teradata.operators.teradata import TeradataOperator
    from airflow.providers.teradata.transfers.teradata_to_teradata import TeradataToTeradataOperator
except ImportError:
    pytest.skip("Teradata provider apache-airflow-provider-teradata not available", allow_module_level=True)


CONN_ID = "teradata_default"


with DAG(
    dag_id="example_teradata_to_teradata_transfer_operator",
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    start_date=datetime(2023, 1, 1),
) as dag:
    # [START howto_transfer_operator_teradata_to_teradata]

    create_src_table = TeradataOperator(
        task_id="create_src_table",
        conn_id=CONN_ID,
        sql="""
            CREATE TABLE my_users_src,
            FALLBACK (
                user_id decimal(10,0) NOT NULL GENERATED ALWAYS AS IDENTITY (
                    START WITH 1
                    INCREMENT BY 1
                    MINVALUE 1
                    MAXVALUE 2147483647
                    NO CYCLE),
                user_name VARCHAR(30),
                gender CHAR(1) DEFAULT 'M',
                birth_date DATE FORMAT 'YYYY-MM-DD' NOT NULL DEFAULT DATE '2023-01-01'
            ) PRIMARY INDEX (user_id);
        """,
    )

    create_dest_table = TeradataOperator(
        task_id="create_dest_table",
        conn_id=CONN_ID,
        sql="""
            CREATE TABLE my_users_dest,
            FALLBACK (
                user_id decimal(10,0) NOT NULL GENERATED ALWAYS AS IDENTITY (
                    START WITH 1
                    INCREMENT BY 1
                    MINVALUE 1
                    MAXVALUE 2147483647
                    NO CYCLE),
                user_name VARCHAR(30),
                gender CHAR(1) DEFAULT 'M',
                birth_date DATE FORMAT 'YYYY-MM-DD' NOT NULL DEFAULT DATE '2023-01-01'
            ) PRIMARY INDEX (user_id);
        """,
    )
    insert_data_src = TeradataOperator(
        task_id="insert_data_src",
        conn_id=CONN_ID,
        sql="""
            INSERT INTO my_users_src(user_name) VALUES ('User1');
            INSERT INTO my_users_src(user_name) VALUES ('User2');
            INSERT INTO my_users_src(user_name) VALUES ('User3');
        """,
    )

    read_data_src = TeradataOperator(
        task_id="read_data_src",
        conn_id=CONN_ID,
        sql="""
            SELECT TOP 10 * from my_users_src order by user_id desc;
        """,
    )

    transfer_data = TeradataToTeradataOperator(
        task_id="transfer_data",
        dest_teradata_conn_id="teradata_default",
        destination_table="my_users_dest",
        source_teradata_conn_id="teradata_default",
        sql="select * from my_users_src",
        sql_params={},
        rows_chunk=2,
    )

    read_data_dest = TeradataOperator(
        task_id="read_data_dest",
        conn_id=CONN_ID,
        sql="""
            SELECT TOP 10 * from my_users_dest order by user_id desc;
        """,
    )

    drop_src_table = TeradataOperator(
        task_id="drop_src_table",
        conn_id=CONN_ID,
        sql="""
            DROP TABLE my_users_src;
        """,
    )

    drop_dest_table = TeradataOperator(
        task_id="drop_dest_table",
        conn_id=CONN_ID,
        sql="""
            DROP TABLE my_users_dest;
        """,
    )

    chain(
        create_src_table,
        create_dest_table,
        insert_data_src,
        read_data_src,
        transfer_data,
        read_data_dest,
        drop_src_table,
        drop_dest_table,
    )

    # Make sure create was done before deleting table
    create_src_table >> drop_src_table
    create_dest_table >> drop_dest_table
    # [END howto_transfer_operator_teradata_to_teradata]
