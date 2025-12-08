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
Example use of Neo4j Sensor with a Neo4j Operator.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from airflow.providers.neo4j.sensors.neo4j import Neo4jSensor

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_neo4j_sensor"

with DAG(
    DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    tags=["example"],
    catchup=False,
) as dag:
    # [START run_query_neo4j_sensor]

    run_query_neo4j_sensor = Neo4jSensor(
        task_id="run_query_neo4j_sensor",
        neo4j_conn_id="neo4j_default",
        cypher="RETURN 1 AS value",
        success=lambda x: x == 1,
        poke_interval=5,
        timeout=60,
    )
    # [END run_query_neo4j_sensor]

    run_query_neo4j_operator = Neo4jOperator(
        task_id="run_query_neo4j_operator",
        neo4j_conn_id="neo4j_default",
        parameters={"name": "Tom Hanks"},
        sql="CREATE (actor {name: $name})",
        dag=dag,
    )

    run_query_neo4j_sensor >> run_query_neo4j_operator

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
