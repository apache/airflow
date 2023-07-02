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

import asyncio
from functools import cached_property
from typing import AsyncIterator

from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class GlueCrawlerCompleteTrigger(BaseTrigger):
    """
    Watches for a glue crawl, triggers when it finishes.

    :param crawler_name: name of the crawler to watch
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(self, crawler_name: str, poll_interval: int, aws_conn_id: str):
        super().__init__()
        self.crawler_name = crawler_name
        self.poll_interval = poll_interval
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict]:
        return (
            # dynamically generate the fully qualified name of the class
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

    async def run(self) -> AsyncIterator[TriggerEvent]:
        async with self.hook.async_conn as client:
            waiter = self.hook.get_waiter("crawler_ready", deferrable=True, client=client)
            while True:
                try:
                    await waiter.wait(
                        Name=self.crawler_name,
                        WaiterConfig={"Delay": self.poll_interval, "MaxAttempts": 1},
                    )
                    break  # we reach this point only if the waiter met a success criteria
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise
                    self.log.info("Status of glue crawl is %s", error.last_response["Crawler"]["State"])
                    await asyncio.sleep(int(self.poll_interval))

        yield TriggerEvent({"status": "success", "message": "Crawl Complete"})
