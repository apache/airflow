# -*- coding: utf-8 -*-
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
#
"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.neo4j_operator import Neo4JOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("neo4j_example", default_args=default_args, schedule_interval="@once")

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)

# [START howto_operator_neo4j]
t3 = Neo4JOperator(
    task_id="RunNeo4JQuery",
    cypher_query="MATCH (n) RETURN id(n)",
    output_filename="myfile.csv",
    fail_on_no_results=True,
    n4j_conn_id="ExampleN4J",
    dag=dag
)
# [END howto_operator_neo4j]

t2 << t1
t3 << t2
