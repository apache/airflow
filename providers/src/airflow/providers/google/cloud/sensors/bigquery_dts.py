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
"""This module contains a Google BigQuery Data Transfer Service sensor."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.bigquery_datatransfer_v1 import TransferState

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery_dts import (
    BiqQueryDataTransferServiceHook,
)
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from google.api_core.retry import Retry

    from airflow.utils.context import Context


class BigQueryDataTransferServiceTransferRunSensor(BaseSensorOperator):
    """
    Waits for Data Transfer Service run to complete.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:BigQueryDataTransferServiceTransferRunSensor`

    :param expected_statuses: The expected state of the operation.
        See:
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
    :param run_id: ID of the transfer run.
    :param transfer_config_id: ID of transfer config to be used.
    :param project_id: The BigQuery project id where the transfer configuration should be
        created. If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :param request_timeout: The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    :return: An ``google.cloud.bigquery_datatransfer_v1.types.TransferRun`` instance.
    """

    template_fields: Sequence[str] = (
        "run_id",
        "transfer_config_id",
        "expected_statuses",
        "project_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        run_id: str,
        transfer_config_id: str,
        expected_statuses: (
            set[str | TransferState | int] | str | TransferState | int
        ) = TransferState.SUCCEEDED,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        retry: Retry | _MethodDefault = DEFAULT,
        request_timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.run_id = run_id
        self.transfer_config_id = transfer_config_id
        self.retry = retry
        self.request_timeout = request_timeout
        self.metadata = metadata
        self.expected_statuses = self._normalize_state_list(expected_statuses)
        self.project_id = project_id
        self.gcp_cloud_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.location = location

    def _normalize_state_list(self, states) -> set[TransferState]:
        states = {states} if isinstance(states, (str, TransferState, int)) else states
        result = set()
        for state in states:
            if isinstance(state, str):
                # The proto.Enum type is indexable (via MetaClass and aliased) but MyPy is not able to
                # infer this https://github.com/python/mypy/issues/8968
                result.add(TransferState[state.upper()])  # type: ignore[misc]
            elif isinstance(state, int):
                result.add(TransferState(state))
            elif isinstance(state, TransferState):
                result.add(state)
            else:
                raise TypeError(
                    f"Unsupported type. "
                    f"Expected: str, int, google.cloud.bigquery_datatransfer_v1.TransferState."
                    f"Current type: {type(state)}"
                )
        return result

    def poke(self, context: Context) -> bool:
        hook = BiqQueryDataTransferServiceHook(
            gcp_conn_id=self.gcp_cloud_conn_id,
            impersonation_chain=self.impersonation_chain,
            location=self.location,
        )
        run = hook.get_transfer_run(
            run_id=self.run_id,
            transfer_config_id=self.transfer_config_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.request_timeout,
            metadata=self.metadata,
        )
        self.log.info("Status of %s run: %s", self.run_id, run.state)

        if run.state in (TransferState.FAILED, TransferState.CANCELLED):
            message = f"Transfer {self.run_id} did not succeed"
            raise AirflowException(message)
        return run.state in self.expected_statuses
