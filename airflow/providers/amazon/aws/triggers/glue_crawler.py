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

from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class GlueCrawlerCompleteTrigger(BaseTrigger):
    """
    Watches for a glue crawl, triggers when it finishes

    :param crawler_name: name of the crawler to watch
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param max_attempt: The maximum number of attempts to be made.
    :param conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(self, crawler_name: str, poll_interval: int, conn_id: str):
        super().__init__()
        self.crawler_name = crawler_name
        self.poll_interval = poll_interval
        self.aws_conn_id = conn_id

    def serialize(self) -> tuple[str, dict]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "crawler_name": self.crawler_name,
                "poll_interval": self.poll_interval,
                "aws_conn_id": self.aws_conn_id,
            },
        )

    @cached_property
    def hook(self) -> GlueCrawlerHook:
        return GlueCrawlerHook(aws_conn_id=self.aws_conn_id)

    async def run(self):
        async with self.hook.async_conn as client:
            await client.get_waiter("crawler_ready").wait(
                Name=self.crawler_name,
                WaiterConfig={
                    "Delay": self.poll_interval,
                },
            )
        return TriggerEvent({"status": "success", "message": "Crawl Complete"})
