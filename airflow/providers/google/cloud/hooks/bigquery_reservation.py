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
"""This module contains a BigQuery Reservation Hook."""
from __future__ import annotations

from typing import Sequence

from google.cloud.bigquery_reservation_v1 import ReservationServiceClient

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class BigQueryReservationServiceHook(GoogleBaseHook):
    """
    Hook for Google Bigquery Reservation API.

    :param gcp_conn_id: The Airflow connection used for GCP credentials.
    :param location: The location of the BigQuery resource.
    :param impersonation_chain: This is the optional service account to impersonate using short term
        credentials.
    """

    hook_name = "Google Bigquery Reservation"

    def __init__(
        self,
        gcp_conn_id: str = GoogleBaseHook.default_conn_name,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self.location = location
        self._client: ReservationServiceClient | None = None

    @staticmethod
    def _convert_gb_to_kb(value: int) -> int:
        """
        Convert GB value to KB.

        :param value: Value to convert
        """
        return value * 1073741824

    def get_client(self) -> ReservationServiceClient:
        """
        Get reservation service client.

        :return: Google Bigquery Reservation client
        """
        if not self._client:
            self._client = ReservationServiceClient(
                credentials=self.get_credentials(), client_info=CLIENT_INFO
            )
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def create_bi_reservation(self, project_id: str, size: int) -> None:
        """
        Create BI Engine reservation

        :param project_id: The name of the project where we want to create/update
            the BI Engine reservation.
        :param size: The BI Engine reservation size in Gb.
        """
        parent = f"projects/{project_id}/locations/{self.location}/biReservation"
        client = self.get_client()
        size = self._convert_gb_to_kb(value=size)

        try:
            bi_reservation = client.get_bi_reservation(name=parent)
            bi_reservation.size = size + bi_reservation.size

            client.update_bi_reservation(bi_reservation=bi_reservation)

            self.log.info("BI Engine reservation {parent} have been updated to {bi_reservation.size}Kb.")
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to create BI engine reservation of {size}.")

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_bi_reservation(self, project_id: str, size: int | None = None) -> None:
        """
        Delete/Update BI Engine reservation with the specified memory size

        :param project_id: The name of the project where we want to delete/update
            the BI Engine reservation.
        :param size: The BI Engine reservation size in Gb.
        """
        parent = f"projects/{project_id}/locations/{self.location}/biReservation"
        client = self.get_client()
        try:
            bi_reservation = client.get_bi_reservation(name=parent)
            if size is not None:
                size = self._convert_gb_to_kb(size)
                bi_reservation.size = max(bi_reservation.size - size, 0)
            else:
                bi_reservation.size = 0

            client.update_bi_reservation(bi_reservation=bi_reservation)
            self.log.info("BI Engine reservation {parent} have been updated to {bi_reservation.size}Kb.")
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Failed to delete BI engine reservation of {size}.")
