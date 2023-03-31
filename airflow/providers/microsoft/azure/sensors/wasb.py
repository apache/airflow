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

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.triggers.wasb import WasbBlobSensorTrigger
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class WasbBlobSensor(BaseSensorOperator):
    """
    Waits for a blob to arrive on Azure Blob Storage.

    :param container_name: Name of the container.
    :param blob_name: Name of the blob.
    :param wasb_conn_id: Reference to the :ref:`wasb connection <howto/connection:wasb>`.
    :param check_options: Optional keyword arguments that
        `WasbHook.check_for_blob()` takes.
    """

    template_fields: Sequence[str] = ("container_name", "blob_name")

    def __init__(
        self,
        *,
        container_name: str,
        blob_name: str,
        wasb_conn_id: str = "wasb_default",
        check_options: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if check_options is None:
            check_options = {}
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.check_options = check_options

    def poke(self, context: Context):
        self.log.info("Poking for blob: %s\n in wasb://%s", self.blob_name, self.container_name)
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        return hook.check_for_blob(self.container_name, self.blob_name, **self.check_options)


class WasbBlobAsyncSensor(WasbBlobSensor):
    """
    Polls asynchronously for the existence of a blob in a WASB container.

    :param container_name: name of the container in which the blob should be searched for
    :param blob_name: name of the blob to check existence for
    :param wasb_conn_id: the connection identifier for connecting to Azure WASB
    :param poke_interval:  polling period in seconds to check for the status
    :param public_read: whether an anonymous public read access should be used. Default is False
    :param timeout: Time, in seconds before the task times out and fails.
    """

    def __init__(
        self,
        *,
        container_name: str,
        blob_name: str,
        wasb_conn_id: str = "wasb_default",
        public_read: bool = False,
        poke_interval: float = 5.0,
        **kwargs: Any,
    ):
        self.container_name = container_name
        self.blob_name = blob_name
        self.poke_interval = poke_interval
        super().__init__(container_name=container_name, blob_name=blob_name, **kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.public_read = public_read

    def execute(self, context: Context) -> None:
        """Defers trigger class to poll for state of the job run until it reaches
        a failure state or success state
        """
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=WasbBlobSensorTrigger(
                container_name=self.container_name,
                blob_name=self.blob_name,
                wasb_conn_id=self.wasb_conn_id,
                public_read=self.public_read,
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, str]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])
        else:
            raise AirflowException("Did not receive valid event from the triggerer")


class WasbPrefixSensor(BaseSensorOperator):
    """
    Waits for blobs matching a prefix to arrive on Azure Blob Storage.

    :param container_name: Name of the container.
    :param prefix: Prefix of the blob.
    :param wasb_conn_id: Reference to the wasb connection.
    :param check_options: Optional keyword arguments that
        `WasbHook.check_for_prefix()` takes.
    """

    template_fields: Sequence[str] = ("container_name", "prefix")

    def __init__(
        self,
        *,
        container_name: str,
        prefix: str,
        wasb_conn_id: str = "wasb_default",
        check_options: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if check_options is None:
            check_options = {}
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.prefix = prefix
        self.check_options = check_options

    def poke(self, context: Context) -> bool:
        self.log.info("Poking for prefix: %s in wasb://%s", self.prefix, self.container_name)
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        return hook.check_for_prefix(self.container_name, self.prefix, **self.check_options)
