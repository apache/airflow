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
"""This module contains Google Cloud Looker sensors."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseSensorOperator
from airflow.providers.google.cloud.hooks.looker import JobStatus, LookerHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class LookerCheckPdtBuildSensor(BaseSensorOperator):
    """
    Check for the state of a previously submitted PDT materialization job.

    :param materialization_id: Required. The materialization job ID to poll. (templated)
    :param looker_conn_id: Required. The connection ID to use connecting to Looker.
    :param cancel_on_kill: Optional. Flag which indicates whether cancel the hook's job or not,
        when on_kill is called.
    """

    template_fields = ["materialization_id"]

    def __init__(
        self, materialization_id: str, looker_conn_id: str, cancel_on_kill: bool = True, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.materialization_id = materialization_id
        self.looker_conn_id = looker_conn_id
        self.cancel_on_kill = cancel_on_kill
        self.hook: LookerHook | None = None

    def poke(self, context: Context) -> bool:
        self.hook = LookerHook(looker_conn_id=self.looker_conn_id)

        if not self.materialization_id:
            message = "Invalid `materialization_id`."
            raise AirflowException(message)

        # materialization_id is templated var pulling output from start task
        status_dict = self.hook.pdt_build_status(materialization_id=self.materialization_id)
        status = status_dict["status"]

        if status == JobStatus.ERROR.value:
            msg = status_dict["message"]
            message = f'PDT materialization job failed. Job id: {self.materialization_id}. Message:\n"{msg}"'
            raise AirflowException(message)
        if status == JobStatus.CANCELLED.value:
            message = f"PDT materialization job was cancelled. Job id: {self.materialization_id}."
            raise AirflowException(message)
        if status == JobStatus.UNKNOWN.value:
            message = f"PDT materialization job has unknown status. Job id: {self.materialization_id}."
            raise AirflowException(message)
        if status == JobStatus.DONE.value:
            self.log.debug(
                "PDT materialization job completed successfully. Job id: %s.", self.materialization_id
            )
            return True

        self.log.info("Waiting for PDT materialization job to complete. Job id: %s.", self.materialization_id)
        return False

    def on_kill(self):
        if self.materialization_id and self.cancel_on_kill:
            self.hook.stop_pdt_build(materialization_id=self.materialization_id)
