# -*- coding: utf-8 -*-
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
This module contains a Google BigQuery Data Transfer Service sensor.
"""
from typing import Optional, Sequence, Set, Tuple, Union

from google.api_core.retry import Retry
from google.protobuf.json_format import MessageToDict

from airflow.gcp.hooks.bigquery_dts import BiqQueryDataTransferServiceHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class BigQueryDataTransferServiceTransferRunSensor(BaseSensorOperator):
    """
    Waits for Data Transfer Service run to complete.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:BigQueryDataTransferServiceTransferRunSensor`

    :param expected_statuses: The expected state of the operation.
        See:
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
    :type expected_statuses: Union[Set[str], str]
    :param run_id: ID of the transfer run.
    :type run_id: str
    :param transfer_config_id: ID of transfer config to be used.
    :type transfer_config_id: str
    :param project_id: The BigQuery project id where the transfer configuration should be
        created. If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param retry: A retry object used to retry requests. If `None` is
        specified, requests will not be retried.
    :type retry: Optional[google.api_core.retry.Retry]
    :param request_timeout: The amount of time, in seconds, to wait for the request to
        complete. Note that if retry is specified, the timeout applies to each individual
        attempt.
    :type request_timeout: Optional[float]
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :return: An ``google.cloud.bigquery_datatransfer_v1.types.TransferRun`` instance.
    """

    template_fields = (
        "run_id",
        "transfer_config_id",
        "expected_statuses",
        "project_id",
    )

    @apply_defaults
    def __init__(
        self,
        run_id: str,
        transfer_config_id: str,
        expected_statuses: Union[Set[str], str] = 'SUCCEEDED',
        project_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        retry: Retry = None,
        request_timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.run_id = run_id
        self.transfer_config_id = transfer_config_id
        self.retry = retry
        self.request_timeout = request_timeout
        self.metadata = metadata
        self.expected_statuses = (
            {expected_statuses}
            if isinstance(expected_statuses, str)
            else expected_statuses
        )
        self.project_id = project_id
        self.gcp_cloud_conn_id = gcp_conn_id

    def poke(self, context):
        hook = BiqQueryDataTransferServiceHook(gcp_conn_id=self.gcp_cloud_conn_id)
        run = hook.get_transfer_run(
            run_id=self.run_id,
            transfer_config_id=self.transfer_config_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.request_timeout,
            metadata=self.metadata,
        )
        result = MessageToDict(run)
        state = result["state"]
        self.log.info("Status of %s run: %s", self.run_id, state)

        return state in self.expected_statuses
