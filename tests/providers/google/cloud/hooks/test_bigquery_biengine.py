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

from random import randint
from unittest import mock

import pytest
from google.cloud.bigquery_reservation_v1 import BiReservation, ReservationServiceClient

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery_biengine import (
    BigQueryReservationServiceHook,
)
from airflow.providers.google.common.consts import CLIENT_INFO
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_no_default_project_id

CREDENTIALS = "test-creds"
PROJECT_ID = "test-project"
LOCATION = "US"
SIZE = 100
CONSTANT_GO_TO_KO = 1073741824
SIZE_KO = SIZE * CONSTANT_GO_TO_KO
PARENT = f"projects/{PROJECT_ID}/locations/{LOCATION}/biReservation"


class TestBigQueryReservationHook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.bigquery_biengine.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = BigQueryReservationServiceHook(location=LOCATION)
            self.hook.get_credentials = mock.MagicMock(return_value=CREDENTIALS)
            self.location = LOCATION

    def test_convert_gb_to_kb(self):
        value = randint(1, 1000)
        assert self.hook._convert_gb_to_kb(value) == value * 1073741824

    @mock.patch("google.cloud.bigquery_reservation_v1.ReservationServiceClient")
    def test_get_client_already_exist(self, reservation_client_mock):
        expected = reservation_client_mock(credentials=self.hook.get_credentials(), client_info=CLIENT_INFO)
        self.hook._client = expected
        assert self.hook.get_client() == expected

    # Create BI Reservation
    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_biengine.BigQueryReservationServiceHook.get_client"
    )
    def test_create_bi_reservation_existing_reservation_success(self, client_mock):
        requested_size_gb = SIZE
        initial_size = SIZE_KO
        expected_size = initial_size + requested_size_gb * CONSTANT_GO_TO_KO

        client_mock.return_value.get_bi_reservation.return_value = BiReservation(
            name=PARENT, size=initial_size
        )
        self.hook.create_bi_reservation(project_id=PROJECT_ID, size=requested_size_gb)
        client_mock.return_value.get_bi_reservation.assert_called_once_with(name=PARENT)
        client_mock.return_value.update_bi_reservation.assert_called_once_with(
            bi_reservation=BiReservation(name=PARENT, size=expected_size)
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_biengine.BigQueryReservationServiceHook.get_client"
    )
    def test_create_bi_reservation_none_existing_reservation_success(self, client_mock):
        requested_size_gb = SIZE
        initial_size = 0
        expected_size = initial_size + requested_size_gb * CONSTANT_GO_TO_KO

        client_mock.return_value.get_bi_reservation.return_value = BiReservation(
            name=PARENT, size=initial_size
        )
        self.hook.create_bi_reservation(project_id=PROJECT_ID, size=requested_size_gb)
        client_mock.return_value.get_bi_reservation.assert_called_once_with(name=PARENT)

        client_mock.return_value.update_bi_reservation.assert_called_once_with(
            bi_reservation=BiReservation(name=PARENT, size=expected_size)
        )

    @mock.patch.object(ReservationServiceClient, "get_bi_reservation", side_effect=Exception("Test"))
    def test_create_bi_reservation_get_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.create_bi_reservation(project_id=PROJECT_ID, size=SIZE)

    @mock.patch.object(ReservationServiceClient, "get_bi_reservation")
    @mock.patch.object(ReservationServiceClient, "update_bi_reservation", side_effect=Exception("Test"))
    def test_create_bi_reservation_update_failure(self, call_failure, get_bi_reservation_mock):
        with pytest.raises(AirflowException):
            self.hook.create_bi_reservation(project_id=PROJECT_ID, size=SIZE)

    # Delete BI Reservation
    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_biengine.BigQueryReservationServiceHook.get_client"
    )
    def test_delete_bi_reservation_size_none_success(self, client_mock):
        initial_size = SIZE_KO
        expected_size = 0
        client_mock.return_value.get_bi_reservation.return_value = BiReservation(
            name=PARENT, size=initial_size
        )
        self.hook.delete_bi_reservation(project_id=PROJECT_ID)
        client_mock.return_value.get_bi_reservation.assert_called_once_with(name=PARENT)
        client_mock.return_value.update_bi_reservation.assert_called_once_with(
            bi_reservation=BiReservation(name=PARENT, size=expected_size)
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_biengine.BigQueryReservationServiceHook.get_client"
    )
    def test_delete_bi_reservation_size_filled_update_non_negative_success(self, client_mock):
        initial_size = 100 * CONSTANT_GO_TO_KO
        requeted_deleted_size_gb = 50
        expected_size = initial_size - requeted_deleted_size_gb * CONSTANT_GO_TO_KO
        client_mock.return_value.get_bi_reservation.return_value = BiReservation(
            name=PARENT, size=initial_size
        )
        self.hook.delete_bi_reservation(project_id=PROJECT_ID, size=requeted_deleted_size_gb)
        client_mock.return_value.get_bi_reservation.assert_called_once_with(name=PARENT)
        client_mock.return_value.update_bi_reservation.assert_called_once_with(
            bi_reservation=BiReservation(name=PARENT, size=expected_size)
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_biengine.BigQueryReservationServiceHook.get_client"
    )
    def test_delete_bi_reservation_size_filled_update_negative_success(self, client_mock):
        initial_size = 100 * CONSTANT_GO_TO_KO
        requeted_deleted_size_gb = 200
        expected_size = 0
        client_mock.return_value.get_bi_reservation.return_value = BiReservation(
            name=PARENT, size=initial_size
        )
        self.hook.delete_bi_reservation(project_id=PROJECT_ID, size=requeted_deleted_size_gb)
        client_mock.return_value.get_bi_reservation.assert_called_once_with(name=PARENT)
        client_mock.return_value.update_bi_reservation.assert_called_once_with(
            bi_reservation=BiReservation(name=PARENT, size=expected_size)
        )

    @mock.patch.object(ReservationServiceClient, "get_bi_reservation", side_effect=Exception("Test"))
    def test_delete_bi_reservation_get_failure(self, call_failure):
        with pytest.raises(AirflowException):
            self.hook.delete_bi_reservation(project_id=PROJECT_ID, size=SIZE)

    @mock.patch.object(ReservationServiceClient, "get_bi_reservation")
    @mock.patch.object(ReservationServiceClient, "update_bi_reservation", side_effect=Exception("Test"))
    def test_delete_bi_reservation_update_failure(self, call_failure, get_bi_reservation_mock):
        with pytest.raises(AirflowException):
            self.hook.delete_bi_reservation(project_id=PROJECT_ID, size=SIZE)
