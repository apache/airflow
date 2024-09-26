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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlueCrawlerSensor(AwsBaseSensor[GlueCrawlerHook]):
    """
    Waits for an AWS Glue crawler to reach any of the statuses below.

    'FAILED', 'CANCELLED', 'SUCCEEDED'

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:GlueCrawlerSensor`

    :param crawler_name: The AWS Glue crawler unique name
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
        "crawler_name",
    )

    def __init__(self, *, crawler_name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.crawler_name = crawler_name
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
                raise AirflowException(f"Status: {crawler_status}")
        else:
            return False
