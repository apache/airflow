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
Example Airflow DAG that performs BI Engine.

This DAG reserves a 100GB BI engine reservation between from 8am ato 7pm each working days.
This DAG executes this following workflow:
    1. Create BI reservation
    2. Wait the precise date
    3. Delete BI reservation
"""
from __future__ import annotations

import os

from pendulum import Time, datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery_reservation import (
    BigQueryBiEngineReservationCreateOperator,
    BigQueryBiEngineReservationDeleteOperator,
)
from airflow.sensors.time_sensor import TimeSensor

END_TIME = Time(19, 0, 0)
BI_ENGINE_RESERVATION_SIZE = 100
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "example-project")
LOCATION = os.getenv("GCP_PROJECT_LOCATION", "US")

with models.DAG(
    "example_bigquery_reservation_bi_engine",
    start_date=datetime(2023, 1, 1),
    default_args={
        "retries": 2,
    },
    tags=["example", "bigquery", "bi_engine"],
    catchup=False,
) as dag:

    create_bi_engine_reservation = BigQueryBiEngineReservationCreateOperator(
        task_id="create_bi_engine_reservation",
        project_id=PROJECT_ID,
        location=LOCATION,
        size=BI_ENGINE_RESERVATION_SIZE,
    )

    wait = TimeSensor(task_id="wait", target_time=END_TIME)

    delete_bi_engine_reservation = BigQueryBiEngineReservationDeleteOperator(
        task_id="delete_bi_engine_reservation",
        project_id=PROJECT_ID,
        location=LOCATION,
        size=BI_ENGINE_RESERVATION_SIZE,
    )

    create_bi_engine_reservation >> wait
    wait >> delete_bi_engine_reservation
