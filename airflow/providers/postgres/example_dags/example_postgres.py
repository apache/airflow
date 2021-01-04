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

import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {"owner": "airflow"}

with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
    )

    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO pet VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )

    get_all_pets = PostgresOperator(
        task_id="get_all_pets", postgres_conn_id="postgres_default", sql="SELECT * FROM pet;"
    )

    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        postgres_conn_id="postgres_default",
        sql="""
            SELECT * FROM pet
            WHERE birth_date
            BETWEEN SYMMETRIC {{ params.begin_date }} AND {{ params.end_date }};
            """,
        params={'begin_date': '2020-01-01', 'end_date': '2020-12-31'},
    )

    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date
