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

from deprecated import deprecated

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.triggers.wasb import (
    WasbBlobSensorTrigger,
    WasbPrefixSensorTrigger,
)
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
    :param deferrable: Run sensor in the deferrable mode.
    :param public_read: whether an anonymous public read access should be used. Default is False
    """

    template_fields: Sequence[str] = ("container_name", "blob_name")

    def __init__(
        self,
        *,
        container_name: str,
        blob_name: str,
        wasb_conn_id: str = "wasb_default",
        check_options: dict | None = None,
        public_read: bool = False,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if check_options is None:
            check_options = {}
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.check_options = check_options
        self.public_read = public_read
        self.deferrable = deferrable

    def poke(self, context: Context):
        self.log.info(
            "Poking for blob: %s\n in wasb://%s", self.blob_name, self.container_name
        )
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        return hook.check_for_blob(
            self.container_name, self.blob_name, **self.check_options
        )

    def execute(self, context: Context) -> None:
        """
        Poll for state of the job run.

        In deferrable mode, the polling is deferred to the triggerer. Otherwise
        the sensor waits synchronously.
        """
        if not self.deferrable:
            super().execute(context=context)
        else:
            if not self.poke(context=context):
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
        Return immediately - callback for when the trigger fires.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])
        else:
            raise AirflowException("Did not receive valid event from the triggerer")


@deprecated(
    reason=(
        "Class `WasbBlobAsyncSensor` is deprecated and "
        "will be removed in a future release. "
        "Please use `WasbBlobSensor` and "
        "set `deferrable` attribute to `True` instead"
    ),
    category=AirflowProviderDeprecationWarning,
)
class WasbBlobAsyncSensor(WasbBlobSensor):
    """
    Poll asynchronously for the existence of a blob in a WASB container.

    This class is deprecated and will be removed in a future release.

    Please use :class:`airflow.providers.microsoft.azure.sensors.wasb.WasbBlobSensor`
    and set *deferrable* attribute to *True* instead.

    :param container_name: name of the container in which the blob should be searched for
    :param blob_name: name of the blob to check existence for
    :param wasb_conn_id: the connection identifier for connecting to Azure WASB
    :param poke_interval:  polling period in seconds to check for the status
    :param public_read: whether an anonymous public read access should be used. Default is False
    :param timeout: Time, in seconds before the task times out and fails.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs, deferrable=True)


class WasbPrefixSensor(BaseSensorOperator):
    """
    Wait for blobs matching a prefix to arrive on Azure Blob Storage.

    :param container_name: Name of the container.
    :param prefix: Prefix of the blob.
    :param wasb_conn_id: Reference to the wasb connection.
    :param check_options: Optional keyword arguments that
        `WasbHook.check_for_prefix()` takes.
    :param public_read: whether an anonymous public read access should be used. Default is False
    :param deferrable: Run operator in the deferrable mode.
    """

    template_fields: Sequence[str] = ("container_name", "prefix")

    def __init__(
        self,
        *,
        container_name: str,
        prefix: str,
        wasb_conn_id: str = "wasb_default",
        check_options: dict | None = None,
        public_read: bool = False,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if check_options is None:
            check_options = {}
        self.container_name = container_name
        self.prefix = prefix
        self.wasb_conn_id = wasb_conn_id
        self.check_options = check_options
        self.public_read = public_read
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        self.log.info(
            "Poking for prefix: %s in wasb://%s", self.prefix, self.container_name
        )
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id, public_read=self.public_read)
        return hook.check_for_prefix(
            self.container_name, self.prefix, **self.check_options
        )

    def execute(self, context: Context) -> None:
        """
        Poll for state of the job run.

        In deferrable mode, the polling is deferred to the triggerer. Otherwise
        the sensor waits synchronously.
        """
        if not self.deferrable:
            super().execute(context=context)
        else:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=WasbPrefixSensorTrigger(
                        container_name=self.container_name,
                        prefix=self.prefix,
                        wasb_conn_id=self.wasb_conn_id,
                        check_options=self.check_options,
                        public_read=self.public_read,
                        poke_interval=self.poke_interval,
                    ),
                    method_name="execute_complete",
                )

    def execute_complete(self, context: Context, event: dict[str, str]) -> None:
        """
        Return immediately - callback for when the trigger fires.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])
        else:
            raise AirflowException("Did not receive valid event from the triggerer")
