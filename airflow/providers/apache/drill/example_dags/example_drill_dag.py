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
Example Airflow DAG to execute SQL in an Apache Drill environment using the `DrillOperator`.
"""
from datetime import datetime

from airflow.models import DAG
from airflow.providers.apache.drill.operators.drill import DrillOperator

with DAG(
    dag_id='example_drill_dag',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_drill]
    sql_task = DrillOperator(
        task_id='json_to_parquet_table',
        sql='''
        drop table if exists dfs.tmp.employee;
        create table dfs.tmp.employee as select * from cp.`employee.json`;
        ''',
    )
    # [END howto_operator_drill]
