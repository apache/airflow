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
Example Airflow DAG for Elasticsearch Query.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook, ElasticsearchSQLHook

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "elasticsearch_dag"
CONN_ID = "elasticsearch_default"


@task(task_id="es_print_tables")
def show_tables():
    """
    show_tables queries elasticsearch to list available tables
    """
    # [START howto_elasticsearch_query]
    es = ElasticsearchSQLHook(elasticsearch_conn_id=CONN_ID)

    es_connection = es.get_conn()
    response = es_connection.execute_sql("SHOW TABLES")
    for row in response["rows"]:
        print(f"row: {row}")
    return True
    # [END howto_elasticsearch_query]


# [START howto_elasticsearch_python_hook]
def use_elasticsearch_hook():
    """
    Use ElasticSearchPythonHook to print results from a local Elasticsearch
    """
    es_hosts = ["http://localhost:9200"]
    es_hook = ElasticsearchPythonHook(hosts=es_hosts)
    query = {"query": {"match_all": {}}}
    result = es_hook.search(query=query)
    print(result)
    return True


# [END howto_elasticsearch_python_hook]


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "elasticsearch"],
) as dag:
    execute_query = show_tables()
    (
        # TEST BODY
        execute_query
    )
    es_python_test = PythonOperator(
        task_id="print_data_from_elasticsearch", python_callable=use_elasticsearch_hook
    )

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
