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

from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.compat.functools import cached_property
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook


class GlueCrawlerOperator(BaseOperator):
    """
    Creates, updates and triggers an AWS Glue Crawler. AWS Glue Crawler is a serverless
    service that manages a catalog of metadata tables that contain the inferred
    schema, format and data types of data stores within the AWS cloud.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlueCrawlerOperator`

    :param config: Configurations for the AWS Glue crawler
    :param aws_conn_id: aws connection to use
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
    :param wait_for_completion: Whether or not wait for crawl execution completion. (default: True)
    """

    template_fields: Sequence[str] = ("config",)
    ui_color = "#ededed"

    def __init__(
        self,
        config,
        aws_conn_id="aws_default",
        region_name: str | None = None,
        poll_interval: int = 5,
        wait_for_completion: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval
        self.wait_for_completion = wait_for_completion
        self.region_name = region_name
        self.config = config

    @cached_property
    def hook(self) -> GlueCrawlerHook:
        """Create and return an GlueCrawlerHook."""
        return GlueCrawlerHook(self.aws_conn_id, region_name=self.region_name)

    def execute(self, context: Context):
        """
        Executes AWS Glue Crawler from Airflow

        :return: the name of the current glue crawler.
        """
        crawler_name = self.config["Name"]
        if self.hook.has_crawler(crawler_name):
            self.hook.update_crawler(**self.config)
        else:
            self.hook.create_crawler(**self.config)

        self.log.info("Triggering AWS Glue Crawler")
        self.hook.start_crawler(crawler_name)
        if self.wait_for_completion:
            self.log.info("Waiting for AWS Glue Crawler")
            self.hook.wait_for_crawler_completion(crawler_name=crawler_name, poll_interval=self.poll_interval)

        return crawler_name
