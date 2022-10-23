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

import warnings
from typing import TYPE_CHECKING, Any, Optional, Sequence

from airflow.compat.functools import cached_property
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.athena import AthenaHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AthenaOperator(BaseOperator):
    """
    An operator that submits a presto query to athena.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AthenaOperator`

    :param query: Presto to be run on athena. (templated)
    :param database: Database to select. (templated)
    :param output_location: s3 path to write the query results into. (templated)
    :param aws_conn_id: aws connection to use
    :param client_request_token: Unique token created by user to avoid multiple executions of same query
    :param workgroup: Athena workgroup in which query will be run
    :param query_execution_context: Context in which query need to be run
    :param result_configuration: Dict with path to store results in and config related to encryption
    :param sleep_time: Time (in seconds) to wait between two consecutive calls to check query status on Athena
    :param max_tries: Deprecated - use max_polling_attempts instead.
    :param max_polling_attempts: Number of times to poll for query state before function exits
        To limit task execution time, use execution_timeout.
    """

    ui_color = "#44b5e2"
    template_fields: Sequence[str] = ("query", "database", "output_location")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"query": "sql"}

    def __init__(
        self,
        *,
        query: str,
        database: str,
        output_location: str,
        aws_conn_id: str = "aws_default",
        client_request_token: str | None = None,
        workgroup: str = "primary",
        query_execution_context: dict[str, str] | None = None,
        result_configuration: dict[str, Any] | None = None,
        sleep_time: int = 30,
        max_tries: int | None = None,
        max_polling_attempts: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.database = database
        self.output_location = output_location
        self.aws_conn_id = aws_conn_id
        self.client_request_token = client_request_token
        self.workgroup = workgroup
        self.query_execution_context = query_execution_context or {}
        self.result_configuration = result_configuration or {}
        self.sleep_time = sleep_time
        self.max_polling_attempts = max_polling_attempts
        self.query_execution_id = None  # type: Optional[str]

        if max_tries:
            warnings.warn(
                f"Parameter `{self.__class__.__name__}.max_tries` is deprecated and will be removed "
                "in a future release.  Please use method `max_polling_attempts` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if max_polling_attempts and max_polling_attempts != max_tries:
                raise Exception("max_polling_attempts must be the same value as max_tries")
            else:
                self.max_polling_attempts = max_tries

    @cached_property
    def hook(self) -> AthenaHook:
        """Create and return an AthenaHook."""
        return AthenaHook(self.aws_conn_id, sleep_time=self.sleep_time)

    def execute(self, context: Context) -> str | None:
        """Run Presto Query on Athena"""
        self.query_execution_context["Database"] = self.database
        self.result_configuration["OutputLocation"] = self.output_location
        self.query_execution_id = self.hook.run_query(
            self.query,
            self.query_execution_context,
            self.result_configuration,
            self.client_request_token,
            self.workgroup,
        )
        query_status = self.hook.poll_query_status(
            self.query_execution_id,
            max_polling_attempts=self.max_polling_attempts,
        )

        if query_status in AthenaHook.FAILURE_STATES:
            error_message = self.hook.get_state_change_reason(self.query_execution_id)
            raise Exception(
                f"Final state of Athena job is {query_status}, query_execution_id is "
                f"{self.query_execution_id}. Error: {error_message}"
            )
        elif not query_status or query_status in AthenaHook.INTERMEDIATE_STATES:
            raise Exception(
                f"Final state of Athena job is {query_status}. Max tries of poll status exceeded, "
                f"query_execution_id is {self.query_execution_id}."
            )

        return self.query_execution_id

    def on_kill(self) -> None:
        """Cancel the submitted athena query"""
        if self.query_execution_id:
            self.log.info("Received a kill signal.")
            self.log.info("Stopping Query with executionId - %s", self.query_execution_id)
            response = self.hook.stop_query(self.query_execution_id)
            http_status_code = None
            try:
                http_status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            except Exception as ex:
                self.log.error("Exception while cancelling query: %s", ex)
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error("Unable to request query cancel on athena. Exiting")
                else:
                    self.log.info(
                        "Polling Athena for query with id %s to reach final state", self.query_execution_id
                    )
                    self.hook.poll_query_status(self.query_execution_id)
