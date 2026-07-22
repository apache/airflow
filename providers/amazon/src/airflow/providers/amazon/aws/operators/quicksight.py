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
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.exceptions import QuickSightIngestionFailedError
from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.quicksight import QuickSightIngestionCompletedTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from airflow.sdk import Context


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
    :param wait_for_completion: If True, wait for the ingestion to reach a terminal state. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 30)
    :param waiter_max_attempts: Maximum number of attempts to check for completion. (default: 60)
    :param check_interval: Deprecated, use ``waiter_delay`` instead.
    :param deferrable: If True, the operator will wait asynchronously for the ingestion to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
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
        "waiter_delay",
        "waiter_max_attempts",
        "check_interval",
    )
    ui_color = "#ffd700"

    def __init__(
        self,
        data_set_id: str,
        ingestion_id: str,
        ingestion_type: str = "FULL_REFRESH",
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        check_interval: int | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        if check_interval is not None:
            warnings.warn(
                "The `check_interval` parameter is deprecated and will be removed in a future release. "
                "Use `waiter_delay` instead. While `check_interval` is set, it takes precedence over "
                "`waiter_delay`.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
        self.data_set_id = data_set_id
        self.ingestion_id = ingestion_id
        self.ingestion_type = ingestion_type
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.check_interval = check_interval
        self.deferrable = deferrable

    def execute(self, context: Context):
        self.log.info("Running the Amazon QuickSight SPICE Ingestion on Dataset ID: %s", self.data_set_id)
        ingestion = self.hook.create_ingestion(
            data_set_id=self.data_set_id,
            ingestion_id=self.ingestion_id,
            ingestion_type=self.ingestion_type,
            wait_for_completion=False,
        )
        # check_interval may be templated, so resolve the deprecated value at execution time
        waiter_delay = int(self.waiter_delay if self.check_interval is None else self.check_interval)
        waiter_max_attempts = int(self.waiter_max_attempts)
        if self.deferrable:
            self.defer(
                trigger=QuickSightIngestionCompletedTrigger(
                    data_set_id=self.data_set_id,
                    ingestion_id=self.ingestion_id,
                    aws_account_id=self.hook.account_id,
                    waiter_delay=waiter_delay,
                    waiter_max_attempts=waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="execute_complete",
                kwargs={"ingestion": ingestion},
            )
        elif self.wait_for_completion:
            self.hook.wait_for_ingestion(
                data_set_id=self.data_set_id,
                ingestion_id=self.ingestion_id,
                waiter_delay=waiter_delay,
                waiter_max_attempts=waiter_max_attempts,
            )
        return ingestion

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None, ingestion: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        validated_event = validate_execute_complete_event(event)
        if validated_event["status"] != "success":
            raise QuickSightIngestionFailedError(
                f"Error while running Amazon QuickSight SPICE ingestion: {validated_event}"
            )
        self.log.info("Amazon QuickSight SPICE ingestion `%s` completed.", validated_event["ingestion_id"])
        return ingestion
