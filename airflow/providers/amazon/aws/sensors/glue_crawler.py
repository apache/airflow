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
from typing import TYPE_CHECKING, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlueCrawlerSensor(BaseSensorOperator):
    """
    Waits for an AWS Glue crawler to reach any of the statuses below
    'FAILED', 'CANCELLED', 'SUCCEEDED'

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:GlueCrawlerSensor`

    :param crawler_name: The AWS Glue crawler unique name
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    """

    template_fields: Sequence[str] = ('crawler_name',)

    def __init__(self, *, crawler_name: str, aws_conn_id: str = 'aws_default', **kwargs) -> None:
        super().__init__(**kwargs)
        self.crawler_name = crawler_name
        self.aws_conn_id = aws_conn_id
        self.success_statuses = 'SUCCEEDED'
        self.errored_statuses = ('FAILED', 'CANCELLED')
        self.hook: Optional[GlueCrawlerHook] = None

    def poke(self, context: 'Context'):
        hook = self.get_hook()
        self.log.info("Poking for AWS Glue crawler: %s", self.crawler_name)
        crawler_state = hook.get_crawler(self.crawler_name)['State']
        if crawler_state == 'READY':
            self.log.info("State: %s", crawler_state)
            crawler_status = hook.get_crawler(self.crawler_name)['LastCrawl']['Status']
            if crawler_status == self.success_statuses:
                self.log.info("Status: %s", crawler_status)
                return True
            else:
                raise AirflowException(f"Status: {crawler_status}")
        else:
            return False

    def get_hook(self) -> GlueCrawlerHook:
        """Returns a new or pre-existing GlueCrawlerHook"""
        if self.hook:
            return self.hook

        self.hook = GlueCrawlerHook(aws_conn_id=self.aws_conn_id)
        return self.hook
