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
"""This module contains Google BigQuery Reservation Service operators."""
from __future__ import annotations

from typing import Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery_reservation import BigQueryReservationServiceHook


class BigQueryBiEngineReservationCreateOperator(BaseOperator):
    """
    Create or Update BI engine reservation.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryBiEngineReservationCreateOperator`

    :param location: The BI engine reservation location. (templated)
    :param size: The BI Engine reservation memory size (GB). (templated)
    :param project_id: (Optional) The name of the project where the reservation
        will be attached from. (templated)
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud. (templated)
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account. (templated)
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    """

    template_fields: Sequence[str] = (
        "project_id",
        "location",
        "size",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        location: str | None,
        size: int,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.size = size
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill

    def execute(self, context: Any) -> None:
        hook = BigQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        hook.create_bi_reservation(project_id=self.project_id, size=self.size)


class BigQueryBiEngineReservationDeleteOperator(BaseOperator):
    """
    Delete or Update BI engine reservation.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryBiEngineReservationDeleteOperator`

    :param project_id: Google Cloud Project where the reservation is attached. (templated)
    :param location: Location where the reservation is attached. (templated)
    :param size: (Optional) BI Engine reservation size to delete (GB).
    :param gcp_conn_id: The connection ID used to connect to Google Cloud. (templated)
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account. (templated)
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    """

    template_fields: Sequence[str] = (
        "project_id",
        "location",
        "size",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str | None,
        location: str | None,
        size: int | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.size = size
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill

    def execute(self, context: Any) -> None:
        hook = BigQueryReservationServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )

        hook.delete_bi_reservation(
            project_id=self.project_id,
            size=self.size,
        )
