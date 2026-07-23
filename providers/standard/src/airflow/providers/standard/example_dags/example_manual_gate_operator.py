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
from __future__ import annotations

import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.manual import ManualGateOperator
from airflow.sdk import DAG

with DAG(
    dag_id="example_manual_gate_operator",
    schedule=None,
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_operator_manual_gate]
    transform = EmptyOperator(task_id="transform")
    load = EmptyOperator(task_id="load")

    run_optional_enrichment = ManualGateOperator(
        task_id="run_optional_enrichment",
        label="Run optional enrichment",
    )

    expensive_enrichment = EmptyOperator(task_id="expensive_enrichment")
    publish_enrichment = EmptyOperator(task_id="publish_enrichment")

    transform >> load
    transform >> run_optional_enrichment >> expensive_enrichment >> publish_enrichment
    # [END howto_operator_manual_gate]
