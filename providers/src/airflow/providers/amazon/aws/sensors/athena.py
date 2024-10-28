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

from typing import TYPE_CHECKING, Any, Sequence

from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.athena import AthenaHook


class AthenaSensor(AwsBaseSensor[AthenaHook]):
    """
    Poll the state of the Query until it reaches a terminal state; fails if the query fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:AthenaSensor`


    :param query_execution_id: query_execution_id to check the state of
    :param max_retries: Number of times to poll for query state before
        returning the current state, defaults to None
    :param sleep_time: Time in seconds to wait between two consecutive call to
        check query status on athena, defaults to 10
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    INTERMEDIATE_STATES = (
        "QUEUED",
        "RUNNING",
    )
    FAILURE_STATES = (
        "FAILED",
        "CANCELLED",
    )
    SUCCESS_STATES = ("SUCCEEDED",)

    aws_hook_class = AthenaHook
    template_fields: Sequence[str] = aws_template_fields(
        "query_execution_id",
    )
    ui_color = "#66c3ff"

    def __init__(
        self,
        *,
        query_execution_id: str,
        max_retries: int | None = None,
        sleep_time: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.query_execution_id = query_execution_id
        self.sleep_time = sleep_time
        self.max_retries = max_retries

    def poke(self, context: Context) -> bool:
        state = self.hook.poll_query_status(
            self.query_execution_id, self.max_retries, self.sleep_time
        )

        if state in self.FAILURE_STATES:
            raise AirflowException("Athena sensor failed")

        if state in self.INTERMEDIATE_STATES:
            return False
        return True
