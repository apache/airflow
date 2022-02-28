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

from typing import TYPE_CHECKING, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

DEFAULT_CONN_ID = "aws_default"


class QuickSightCreateIngestionOperator(BaseOperator):
    """
    Creates and starts a new SPICE ingestion for a dataset.
    Also, helps to Refresh existing SPICE datasets

    :param data_set_id:  ID of the dataset used in the ingestion.
    :param ingestion_id: ID for the ingestion.
    :param aws_account_id: Amazon Web Services account ID.
    :param ingestion_type: Type of ingestion. Values Can be  INCREMENTAL_REFRESH or FULL_REFRESH.
        Default FULL_REFRESH.
    :param wait_for_completion: If wait is set to True, the time interval, in seconds,
        that the operation waits to check the status of the QuickSight Ingestion.
    :param check_interval: if wait is set to be true, this is the time interval
        in seconds which the operator will check the status of the QuickSight Ingestion
    :param max_ingestion_time: If wait is set to True, the operation fails if the Ingestion
        doesn"t finish within max_ingestion_time seconds. If you set this parameter to None,
        the operation does not timeout.
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :param region: Which AWS region the connection should use. (templated)
         If this is None or empty then the default boto3 behaviour is used.
    """

    template_fields: Sequence[str] = (
        "data_set_id",
        "ingestion_id",
        "aws_account_id",
        "ingestion_type",
        "wait_for_completion",
        "check_interval",
        "max_ingestion_time",
        "aws_conn_id",
        "region",
    )
    ui_color = "#ffd700"

    def __init__(
        self,
        data_set_id: str,
        ingestion_id: str,
        aws_account_id: str,
        ingestion_type: str = "FULL_REFRESH",
        wait_for_completion: bool = True,
        check_interval: int = 30,
        max_ingestion_time: Optional[int] = None,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ):
        self.data_set_id = data_set_id
        self.ingestion_id = ingestion_id
        self.aws_account_id = aws_account_id
        self.ingestion_type = ingestion_type
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def execute(self, context: "Context"):
        hook = QuickSightHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )
        self.log.info("Running the QuickSight Spice Ingestion on Dataset ID: %s)", self.data_set_id)
        create_ingestion_response = hook.create_ingestion(
            data_set_id=self.data_set_id,
            ingestion_id=self.ingestion_id,
            aws_account_id=self.aws_account_id,
            ingestion_type=self.ingestion_type,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        return create_ingestion_response
