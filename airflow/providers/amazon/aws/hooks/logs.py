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
"""
This module contains a hook (AwsLogsHook) with some very basic
functionality for interacting with AWS CloudWatch.
"""
from __future__ import annotations

from typing import Generator

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AwsLogsHook(AwsBaseHook):
    """
    Interact with AWS CloudWatch Logs

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "logs"
        super().__init__(*args, **kwargs)

    def get_log_events(
        self,
        log_group: str,
        log_stream_name: str,
        start_time: int = 0,
        skip: int = 0,
        start_from_head: bool = True,
    ) -> Generator:
        """
        A generator for log items in a single stream. This will yield all the
        items that are available at the current moment.

        :param log_group: The name of the log group.
        :param log_stream_name: The name of the specific stream.
        :param start_time: The time stamp value to start reading the logs from (default: 0).
        :param skip: The number of log entries to skip at the start (default: 0).
            This is for when there are multiple entries at the same timestamp.
        :param start_from_head: whether to start from the beginning (True) of the log or
            at the end of the log (False).
        :rtype: dict
        :return: | A CloudWatch log event with the following key-value pairs:
                 |   'timestamp' (int): The time in milliseconds of the event.
                 |   'message' (str): The log event data.
                 |   'ingestionTime' (int): The time in milliseconds the event was ingested.
        """
        next_token = None
        while True:
            if next_token is not None:
                token_arg: dict[str, str] | None = {"nextToken": next_token}
            else:
                token_arg = {}

            response = self.get_conn().get_log_events(
                logGroupName=log_group,
                logStreamName=log_stream_name,
                startTime=start_time,
                startFromHead=start_from_head,
                **token_arg,
            )

            events = response["events"]
            event_count = len(events)

            if event_count > skip:
                events = events[skip:]
                skip = 0
            else:
                skip -= event_count
                events = []

            yield from events

            if next_token != response["nextForwardToken"]:
                next_token = response["nextForwardToken"]
            else:
                return
