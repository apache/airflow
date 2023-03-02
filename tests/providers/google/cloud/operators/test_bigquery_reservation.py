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
from __future__ import annotations

import unittest
from unittest import mock

from airflow.providers.google.cloud.operators.bigquery_reservation import (
    BigQueryBiEngineReservationCreateOperator,
    BigQueryBiEngineReservationDeleteOperator,
)

TASK_ID = "id"
PROJECT_ID = "test-project"
LOCATION = "US"
SIZE = 100


class TestBigQueryBiEngineReservationCreateOperator:
    @mock.patch(
        "airflow.providers.google.cloud.operators.bigquery_reservation.BigQueryReservationServiceHook"
    )
    def test_execute(self, hook_mock):
        operator = BigQueryBiEngineReservationCreateOperator(
            project_id=PROJECT_ID,
            task_id=TASK_ID,
            location=LOCATION,
            size=SIZE,
        )

        operator.execute(None)

        hook_mock.return_value.create_bi_reservation.assert_called_once_with(
            project_id=PROJECT_ID,
            size=SIZE,
        )


class TestBigQueryBiEngineReservationDeleteOperator(unittest.TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.bigquery_reservation.BigQueryReservationServiceHook"
    )
    def test_execute(self, hook_mock):
        operator = BigQueryBiEngineReservationDeleteOperator(
            project_id=PROJECT_ID,
            task_id=TASK_ID,
            location=LOCATION,
            size=SIZE,
        )

        operator.execute(None)

        hook_mock.return_value.delete_bi_reservation.assert_called_once_with(
            project_id=PROJECT_ID,
            size=SIZE,
        )
