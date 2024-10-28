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

from typing import TYPE_CHECKING, Sequence

from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class QuickSightCreateIngestionOperator(AwsBaseOperator[QuickSightHook]):
    """
    Creates and starts a new SPICE ingestion for a dataset;  also helps to Refresh existing SPICE datasets.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:QuickSightCreateIngestionOperator`

    :param data_set_id:  ID of the dataset used in the ingestion.
    :param ingestion_id: ID for the ingestion.
    :param ingestion_type: Type of ingestion. Values Can be  INCREMENTAL_REFRESH or FULL_REFRESH.
        Default FULL_REFRESH.
    :param wait_for_completion: If wait is set to True, the time interval, in seconds,
        that the operation waits to check the status of the Amazon QuickSight Ingestion.
    :param check_interval: if wait is set to be true, this is the time interval
        in seconds which the operator will check the status of the Amazon QuickSight Ingestion
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

    aws_hook_class = QuickSightHook
    template_fields: Sequence[str] = aws_template_fields(
        "data_set_id",
        "ingestion_id",
        "ingestion_type",
        "wait_for_completion",
        "check_interval",
    )
    ui_color = "#ffd700"

    def __init__(
        self,
        data_set_id: str,
        ingestion_id: str,
        ingestion_type: str = "FULL_REFRESH",
        wait_for_completion: bool = True,
        check_interval: int = 30,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.data_set_id = data_set_id
        self.ingestion_id = ingestion_id
        self.ingestion_type = ingestion_type
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval

    def execute(self, context: Context):
        self.log.info(
            "Running the Amazon QuickSight SPICE Ingestion on Dataset ID: %s",
            self.data_set_id,
        )
        return self.hook.create_ingestion(
            data_set_id=self.data_set_id,
            ingestion_id=self.ingestion_id,
            ingestion_type=self.ingestion_type,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
        )
