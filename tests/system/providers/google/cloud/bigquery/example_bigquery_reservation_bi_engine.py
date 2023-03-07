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
Example Airflow DAG for Google BigQuery BI Engine Reservation.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery_biengine import (
    BigQueryBiEngineReservationCreateOperator,
    BigQueryBiEngineReservationDeleteOperator,
)

BI_ENGINE_RESERVATION_SIZE = 100
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "example-project")
LOCATION = os.getenv("GCP_PROJECT_LOCATION", "US")


with models.DAG(
    "example_bigquery_reservation_bi_engine",
    start_date=datetime(2023, 1, 1),
    schedule="@once",
    default_args={
        "retries": 2,
    },
    tags=["example", "bigquery", "bi_engine"],
    catchup=False,
) as dag:

    # [START howto_operator_bigquery_create_bi_engine_reservation]
    create_bi_engine_reservation = BigQueryBiEngineReservationCreateOperator(
        task_id="create_bi_engine_reservation",
        project_id=PROJECT_ID,
        location=LOCATION,
        size=BI_ENGINE_RESERVATION_SIZE,
    )
    # [END howto_operator_bigquery_create_bi_engine_reservation]

    # [START howto_operator_bigquery_delete_bi_engine_reservation]
    delete_bi_engine_reservation = BigQueryBiEngineReservationDeleteOperator(
        task_id="delete_bi_engine_reservation",
        project_id=PROJECT_ID,
        location=LOCATION,
        size=BI_ENGINE_RESERVATION_SIZE,
    )
    # [END howto_operator_bigquery_delete_bi_engine_reservation]

    create_bi_engine_reservation >> delete_bi_engine_reservation

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
