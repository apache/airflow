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

import time
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.standard.operators import BaseOperator
from airflow.providers.common.sql.hooks.handlers import fetch_one_handler
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class SnowparkContainerJobOperator(BaseOperator):
    """
    Execute a job on Snowpark Container Services.

    Submits a container job to a compute pool via ``EXECUTE JOB SERVICE``,
    optionally polls for completion, retrieves container logs, and
    drops the job service on success.

    .. seealso::
        `Snowpark Container Services <https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview>`_

    :param compute_pool: name of the compute pool to run the job on.
    :param container_name: container name as defined in the service specification file,
        used for retrieving logs via ``SYSTEM$GET_SERVICE_LOGS``.
    :param spec: spec filename on the stage (e.g. ``'spec.yaml'``).
        Must be provided together with ``spec_stage``.
    :param spec_stage: stage where the spec file is stored (e.g. ``'@my_stage'``).
        Must be provided together with ``spec``.
    :param spec_text: inline YAML spec text, as an alternative to ``spec``/``spec_stage``.
        The text is wrapped in ``$$`` delimiters automatically.
    :param name: (Optional) job service name. If not provided, Snowflake
        auto-generates a name.
    :param query_warehouse: (Optional) warehouse for SQL queries run inside the container.
        This is passed to ``EXECUTE JOB SERVICE`` and is separate from the ``warehouse``
        parameter used by the operator's own SQL commands.
    :param replicas: (Optional) number of job replicas to run. (default value: 1)
    :param wait_for_completion: poll until the job reaches a terminal state.
        When disabled, the job is submitted and the operator returns
        immediately. (default value: True)
    :param drop_on_completion: drop the job service via ``DROP SERVICE``
        after the job finishes successfully. Failed jobs are not dropped,
        allowing inspection via ``DESCRIBE SERVICE``. (default value: True)
    :param poll_interval: the interval in seconds to poll the query status.
        (default value: 10)
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param database: name of database (will overwrite database defined
        in connection).
    :param schema: name of schema (will overwrite schema defined in
        connection).
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON).
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON). Used for the operator's
        own SQL commands, not for the container's queries.
    """

    template_fields: Sequence[str] = (
        "compute_pool",
        "spec",
        "spec_stage",
        "container_name",
        "spec_text",
        "name",
        "query_warehouse",
        "snowflake_conn_id",
    )

    def __init__(
        self,
        *,
        compute_pool: str,
        container_name: str,
        spec: str | None = None,
        spec_stage: str | None = None,
        spec_text: str | None = None,
        name: str | None = None,
        query_warehouse: str | None = None,
        replicas: int = 1,
        wait_for_completion: bool = True,
        drop_on_completion: bool = True,
        poll_interval: int = 10,
        snowflake_conn_id: str = "snowflake_default",
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        warehouse: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.compute_pool = compute_pool
        self.container_name = container_name
        self.spec = spec
        self.spec_stage = spec_stage
        self.spec_text = spec_text
        self.name = name
        self.query_warehouse = query_warehouse
        self.replicas = replicas
        self.wait_for_completion = wait_for_completion
        self.drop_on_completion = drop_on_completion
        self.poll_interval = poll_interval
        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.role = role
        self.warehouse = warehouse
        self.job_name = None

        if self.spec_text and (self.spec or self.spec_stage):
            raise ValueError("Cannot specify both 'spec_text' and 'spec'/'spec_stage'")
        if not self.spec_text and not (self.spec and self.spec_stage):
            raise ValueError("Must provide either 'spec_text' or both 'spec' and 'spec_stage'")

    @cached_property
    def _hook(self) -> SnowflakeHook:
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role,
        )

    def _build_sql(self) -> str:
        """Build the execute job SQL statement."""
        sql = f"EXECUTE JOB SERVICE IN COMPUTE POOL {self.compute_pool}"
        if self.name:
            sql += f" NAME = {self.name}"
        sql += " ASYNC = TRUE"
        if self.replicas > 1:
            sql += f" REPLICAS = {self.replicas}"
        if self.query_warehouse:
            sql += f" QUERY_WAREHOUSE = {self.query_warehouse}"
        if self.spec_text:
            sql += f" FROM SPECIFICATION $${self.spec_text}$$"
        else:
            sql += f" FROM {self.spec_stage} SPEC = '{self.spec}'"
        return sql

    def _submit_job(self) -> None:
        """Submit the job and set the job name."""
        sql = self._build_sql()
        response = self._hook.run(sql, handler=fetch_one_handler)[0]
        self.job_name = response.split("'")[1]

    def _poll_for_status(self) -> str:
        """Poll until the job reaches a terminal state."""
        while True:
            response = self._hook.run(
                f"DESCRIBE SERVICE {self.job_name}", handler=fetch_one_handler, return_dictionaries=True
            )
            status = response.get("status")
            if status in ("DONE", "FAILED", "CANCELLED", "INTERNAL_ERROR"):
                return status
            if status not in ("PENDING", "RUNNING", "CANCELLING", "SUSPENDING", "DELETING"):
                raise RuntimeError(f"Job {self.job_name} returned unexpected status: {status}")
            time.sleep(self.poll_interval)

    def _log_container_output(self, status: str) -> None:
        """Fetch and log container output for all replicas."""
        for instance_id in range(self.replicas):
            sql = f"SELECT SYSTEM$GET_SERVICE_LOGS('{self.job_name}', {instance_id}, '{self.container_name}')"
            response = self._hook.run(sql, handler=fetch_one_handler)[0]
            if status != "DONE":
                self.log.error("Logs for instance_id %d:\n%s", instance_id, response)
            else:
                self.log.info("Logs for instance_id %d:\n%s", instance_id, response)

    def on_kill(self) -> None:
        """Drop the running service on task kill."""
        if self.job_name:
            try:
                self._hook.run(f"DROP SERVICE IF EXISTS {self.job_name}")
            except Exception as e:
                self.log.error("Error dropping service %s: %s", self.job_name, e)

    def execute(self, context: Context) -> str:
        """Submit and optionally wait for a Snowpark Container Services job."""
        self._submit_job()
        if not self.wait_for_completion:
            return self.job_name
        status = self._poll_for_status()
        self._log_container_output(status)
        if status != "DONE":
            raise RuntimeError(f"Job '{self.job_name}' finished with status: {status}")
        if self.drop_on_completion:
            self._hook.run(f"DROP SERVICE IF EXISTS {self.job_name}")
        return self.job_name
