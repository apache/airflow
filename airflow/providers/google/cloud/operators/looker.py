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
"""This module contains Google Cloud Looker operators."""
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.looker import LookerHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LookerStartPdtBuildOperator(BaseOperator):
    """
    Submits a PDT materialization job to Looker.

    :param looker_conn_id: Required. The connection ID to use connecting to Looker.
    :param model: Required. The model of the PDT to start building.
    :param view: Required. The view of the PDT to start building.
    :param query_params: Optional. Additional materialization parameters.
    :param asynchronous: Optional. Flag indicating whether to wait for the job
        to finish or return immediately.
        This is useful for submitting long running jobs and
        waiting on them asynchronously using the LookerCheckPdtBuildSensor
    :param cancel_on_kill: Optional. Flag which indicates whether cancel the
        hook's job or not, when on_kill is called.
    :param wait_time: Optional. Number of seconds between checks for job to be
        ready. Used only if ``asynchronous`` is False.
    :param wait_timeout: Optional. How many seconds wait for job to be ready.
        Used only if ``asynchronous`` is False.
    """

    def __init__(
        self,
        looker_conn_id: str,
        model: str,
        view: str,
        query_params: dict | None = None,
        asynchronous: bool = False,
        cancel_on_kill: bool = True,
        wait_time: int = 10,
        wait_timeout: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.model = model
        self.view = view
        self.query_params = query_params
        self.looker_conn_id = looker_conn_id
        self.asynchronous = asynchronous
        self.cancel_on_kill = cancel_on_kill
        self.wait_time = wait_time
        self.wait_timeout = wait_timeout
        self.hook: LookerHook | None = None
        self.materialization_id: str | None = None

    def execute(self, context: Context) -> str:

        self.hook = LookerHook(looker_conn_id=self.looker_conn_id)

        resp = self.hook.start_pdt_build(
            model=self.model,
            view=self.view,
            query_params=self.query_params,
        )

        self.materialization_id = resp.materialization_id

        if not self.materialization_id:
            raise AirflowException(
                f"No `materialization_id` was returned for model: {self.model}, view: {self.view}."
            )

        self.log.info("PDT materialization job submitted successfully. Job id: %s.", self.materialization_id)

        if not self.asynchronous:
            self.hook.wait_for_job(
                materialization_id=self.materialization_id,
                wait_time=self.wait_time,
                timeout=self.wait_timeout,
            )

        return self.materialization_id

    def on_kill(self):
        if self.materialization_id and self.cancel_on_kill:
            self.hook.stop_pdt_build(materialization_id=self.materialization_id)
