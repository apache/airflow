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
import sys
import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.utils.context import Context


if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook


class GlueCrawlerOperator(BaseOperator):
    """
    Creates, updates and triggers an AWS Glue Crawler. AWS Glue Crawler is a serverless
    service that manages a catalog of metadata tables that contain the inferred
    schema, format and data types of data stores within the AWS cloud.

    :param config: Configurations for the AWS Glue crawler
    :param aws_conn_id: aws connection to use
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
    """

    ui_color = '#ededed'

    def __init__(
        self,
        config,
        aws_conn_id='aws_default',
        poll_interval: int = 5,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval
        self.config = config

    @cached_property
    def hook(self) -> GlueCrawlerHook:
        """Create and return an GlueCrawlerHook."""
        return GlueCrawlerHook(self.aws_conn_id)

    def execute(self, context: 'Context'):
        """
        Executes AWS Glue Crawler from Airflow

        :return: the name of the current glue crawler.
        """
        crawler_name = self.config['Name']
        if self.hook.has_crawler(crawler_name):
            self.hook.update_crawler(**self.config)
        else:
            self.hook.create_crawler(**self.config)

        self.log.info("Triggering AWS Glue Crawler")
        self.hook.start_crawler(crawler_name)
        self.log.info("Waiting for AWS Glue Crawler")
        self.hook.wait_for_crawler_completion(crawler_name=crawler_name, poll_interval=self.poll_interval)

        return crawler_name


class AwsGlueCrawlerOperator(GlueCrawlerOperator):
    """
    This operator is deprecated.
    Please use :class:`airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerOperator`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This operator is deprecated. "
            "Please use :class:`airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerOperator`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
