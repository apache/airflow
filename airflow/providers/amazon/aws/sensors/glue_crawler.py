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

from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from deprecated import deprecated

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning, AirflowSkipException
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlueCrawlerSensor(BaseSensorOperator):
    """
    Waits for an AWS Glue crawler to reach any of the statuses below.

    'FAILED', 'CANCELLED', 'SUCCEEDED'

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:GlueCrawlerSensor`

    :param crawler_name: The AWS Glue crawler unique name
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    """

    template_fields: Sequence[str] = ("crawler_name",)

    def __init__(self, *, crawler_name: str, aws_conn_id: str | None = "aws_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.crawler_name = crawler_name
        self.aws_conn_id = aws_conn_id
        self.success_statuses = "SUCCEEDED"
        self.errored_statuses = ("FAILED", "CANCELLED")

    def poke(self, context: Context):
        self.log.info("Poking for AWS Glue crawler: %s", self.crawler_name)
        crawler_state = self.hook.get_crawler(self.crawler_name)["State"]
        if crawler_state == "READY":
            self.log.info("State: %s", crawler_state)
            crawler_status = self.hook.get_crawler(self.crawler_name)["LastCrawl"]["Status"]
            if crawler_status == self.success_statuses:
                self.log.info("Status: %s", crawler_status)
                return True
            else:
                # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
                message = f"Status: {crawler_status}"
                if self.soft_fail:
                    raise AirflowSkipException(message)
                raise AirflowException(message)
        else:
            return False

    @deprecated(reason="use `hook` property instead.", category=AirflowProviderDeprecationWarning)
    def get_hook(self) -> GlueCrawlerHook:
        """Return a new or pre-existing GlueCrawlerHook."""
        return self.hook

    @cached_property
    def hook(self) -> GlueCrawlerHook:
        return GlueCrawlerHook(aws_conn_id=self.aws_conn_id)
