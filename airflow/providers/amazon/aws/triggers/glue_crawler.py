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

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class GlueCrawlerCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Watches for a glue crawl, triggers when it finishes.

    :param crawler_name: name of the crawler to watch
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        crawler_name: str,
        aws_conn_id: str | None = "aws_default",
        waiter_delay: int = 5,
        waiter_max_attempts: int = 1500,
        **kwargs,
    ):
        super().__init__(
            serialized_fields={"crawler_name": crawler_name},
            waiter_name="crawler_ready",
            waiter_args={"Name": crawler_name},
            failure_message="Error while waiting for glue crawl to complete",
            status_message="Status of glue crawl is",
            status_queries=["Crawler.State", "Crawler.LastCrawl"],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return GlueCrawlerHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )
