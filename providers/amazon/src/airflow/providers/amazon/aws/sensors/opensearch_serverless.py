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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.opensearch_serverless import OpenSearchServerlessHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.opensearch_serverless import (
    OpenSearchServerlessCollectionActiveTrigger,
)
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.helpers import exactly_one

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OpenSearchServerlessCollectionActiveSensor(AwsBaseSensor[OpenSearchServerlessHook]):
    """
    Poll the state of the Collection until it reaches a terminal state; fails if the query fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:OpenSearchServerlessCollectionAvailableSensor`

    :param collection_id: A collection ID. You can't provide a name and an ID in the same request.
    :param collection_name: A collection name. You can't provide a name and an ID in the same request.

    :param deferrable: If True, the sensor will operate in deferrable more. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 10)
    :param max_retries: Number of times before returning the current state (default: 60)
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

    INTERMEDIATE_STATES = ("CREATING",)
    FAILURE_STATES = (
        "DELETING",
        "FAILED",
    )
    SUCCESS_STATES = ("ACTIVE",)
    FAILURE_MESSAGE = "OpenSearch Serverless Collection sensor failed"

    aws_hook_class = OpenSearchServerlessHook
    template_fields: Sequence[str] = aws_template_fields(
        "collection_id",
        "collection_name",
    )
    ui_color = "#66c3ff"

    def __init__(
        self,
        *,
        collection_id: str | None = None,
        collection_name: str | None = None,
        poke_interval: int = 10,
        max_retries: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if not exactly_one(collection_id is None, collection_name is None):
            raise AttributeError("Either collection_ids or collection_names must be provided, not both.")
        self.collection_id = collection_id
        self.collection_name = collection_name

        self.poke_interval = poke_interval
        self.max_retries = max_retries
        self.deferrable = deferrable

    def poke(self, context: Context, **kwargs) -> bool:
        call_args = (
            {"ids": [str(self.collection_id)]}
            if self.collection_id
            else {"names": [str(self.collection_name)]}
        )
        state = self.hook.conn.batch_get_collection(**call_args)["collectionDetails"][0]["status"]

        if state in self.FAILURE_STATES:
            raise AirflowException(self.FAILURE_MESSAGE)

        if state in self.INTERMEDIATE_STATES:
            return False
        return True

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=OpenSearchServerlessCollectionActiveTrigger(
                    collection_id=self.collection_id,
                    collection_name=self.collection_name,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="poke",
            )
        else:
            super().execute(context=context)
