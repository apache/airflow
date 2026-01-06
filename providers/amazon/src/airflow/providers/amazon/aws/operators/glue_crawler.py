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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.glue_crawler import GlueCrawlerCompleteTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException, conf

if TYPE_CHECKING:
    from airflow.sdk import Context


class GlueCrawlerOperator(AwsBaseOperator[GlueCrawlerHook]):
    """
    Creates, updates and triggers an AWS Glue Crawler.

    AWS Glue Crawler is a serverless service that manages a catalog of
    metadata tables that contain the inferred schema, format and data
    types of data stores within the AWS cloud.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCrawlerOperator`

    :param config: Configurations for the AWS Glue crawler
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
    :param wait_for_completion: Whether to wait for crawl execution completion. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the crawl to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
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

    aws_hook_class = GlueCrawlerHook

    template_fields: Sequence[str] = aws_template_fields(
        "config",
    )
    ui_color = "#ededed"

    def __init__(
        self,
        config,
        poll_interval: int = 5,
        wait_for_completion: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.poll_interval = poll_interval
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.config = config

    def execute(self, context: Context) -> str:
        """
        Execute AWS Glue Crawler from Airflow.

        :return: the name of the current glue crawler.
        """
        crawler_name = self.config["Name"]
        if self.hook.has_crawler(crawler_name):
            self.hook.update_crawler(**self.config)
        else:
            self.hook.create_crawler(**self.config)

        self.log.info("Triggering AWS Glue Crawler")
        self.hook.start_crawler(crawler_name)
        if self.deferrable:
            self.defer(
                trigger=GlueCrawlerCompleteTrigger(
                    crawler_name=crawler_name,
                    waiter_delay=self.poll_interval,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    verify=self.verify,
                    botocore_config=self.botocore_config,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_completion:
            self.log.info("Waiting for AWS Glue Crawler")
            self.hook.wait_for_crawler_completion(crawler_name=crawler_name, poll_interval=self.poll_interval)

        return crawler_name

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Error in glue crawl: {validated_event}")
        return self.config["Name"]
