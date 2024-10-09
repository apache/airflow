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

import asyncio
from typing import Any, AsyncGenerator, Generator

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.helpers import prune_dict

# Guidance received from the AWS team regarding the correct way to check for the end of a stream is that the
# value of the nextForwardToken is the same in subsequent calls.
# The issue with this approach is, it can take a huge amount of time (e.g. 20 seconds) to retrieve logs using
# this approach. As an intermediate solution, we decided to stop fetching logs if 3 consecutive responses
# are empty.
# See PR https://github.com/apache/airflow/pull/20814
NUM_CONSECUTIVE_EMPTY_RESPONSE_EXIT_THRESHOLD = 3


class AwsLogsHook(AwsBaseHook):
    """
    Interact with Amazon CloudWatch Logs.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("logs") <CloudWatchLogs.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "logs"
        super().__init__(*args, **kwargs)

    class ContinuationToken:
        """Just a wrapper around a str token to allow updating it from the caller."""

        def __init__(self):
            self.value: str | None = None

    def get_log_events(
        self,
        log_group: str,
        log_stream_name: str,
        start_time: int = 0,
        skip: int = 0,
        start_from_head: bool | None = None,
        continuation_token: ContinuationToken | None = None,
        end_time: int | None = None,
    ) -> Generator:
        """
        Return a generator for log items in a single stream; yields all items available at the current moment.

        .. seealso::
            - :external+boto3:py:meth:`CloudWatchLogs.Client.get_log_events`

        :param log_group: The name of the log group.
        :param log_stream_name: The name of the specific stream.
        :param start_time: The timestamp value in ms to start reading the logs from (default: 0).
        :param skip: The number of log entries to skip at the start (default: 0).
            This is for when there are multiple entries at the same timestamp.
        :param continuation_token: a token indicating where to read logs from.
            Will be updated as this method reads new logs, to be reused in subsequent calls.
        :param end_time: The timestamp value in ms to stop reading the logs from (default: None).
            If None is provided, reads it until the end of the log stream
        :return: | A CloudWatch log event with the following key-value pairs:
                 |   'timestamp' (int): The time in milliseconds of the event.
                 |   'message' (str): The log event data.
                 |   'ingestionTime' (int): The time in milliseconds the event was ingested.
        """
        if continuation_token is None:
            continuation_token = AwsLogsHook.ContinuationToken()

        num_consecutive_empty_response = 0
        while True:
            if continuation_token.value is not None:
                token_arg: dict[str, str] = {"nextToken": continuation_token.value}
            else:
                token_arg = {}

            response = self.conn.get_log_events(
                **prune_dict(
                    {
                        "logGroupName": log_group,
                        "logStreamName": log_stream_name,
                        "startTime": start_time,
                        "endTime": end_time,
                        "startFromHead": True,
                        **token_arg,
                    }
                )
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

            if continuation_token.value == response["nextForwardToken"]:
                return

            if not event_count:
                num_consecutive_empty_response += 1
                if num_consecutive_empty_response >= NUM_CONSECUTIVE_EMPTY_RESPONSE_EXIT_THRESHOLD:
                    # Exit if there are more than NUM_CONSECUTIVE_EMPTY_RESPONSE_EXIT_THRESHOLD consecutive
                    # empty responses
                    return
            else:
                num_consecutive_empty_response = 0

            continuation_token.value = response["nextForwardToken"]

    async def describe_log_streams_async(
        self, log_group: str, stream_prefix: str, order_by: str, count: int
    ) -> dict[str, Any] | None:
        """
        Async function to get the list of log streams for the specified log group.

        You can list all the log streams or filter the results by prefix. You can also control
        how the results are ordered.

        :param log_group: The name of the log group.
        :param stream_prefix: The prefix to match.
        :param order_by: If the value is LogStreamName , the results are ordered by log stream name.
         If the value is LastEventTime , the results are ordered by the event time. The default value is LogStreamName.
        :param count: The maximum number of items returned
        """
        async with self.async_conn as client:
            try:
                response: dict[str, Any] = await client.describe_log_streams(
                    logGroupName=log_group,
                    logStreamNamePrefix=stream_prefix,
                    orderBy=order_by,
                    limit=count,
                )
                return response
            except ClientError as error:
                # On the very first training job run on an account, there's no log group until
                # the container starts logging, so ignore any errors thrown about that
                if error.response["Error"]["Code"] == "ResourceNotFoundException":
                    return None
                raise error

    async def get_log_events_async(
        self,
        log_group: str,
        log_stream_name: str,
        start_time: int = 0,
        skip: int = 0,
        start_from_head: bool = True,
    ) -> AsyncGenerator[Any, dict[str, Any]]:
        """
        Yield all the available items in a single log stream.

        :param log_group: The name of the log group.
        :param log_stream_name: The name of the specific stream.
        :param start_time: The time stamp value to start reading the logs from (default: 0).
        :param skip: The number of log entries to skip at the start (default: 0).
            This is for when there are multiple entries at the same timestamp.
        :param start_from_head: whether to start from the beginning (True) of the log or
            at the end of the log (False).
        """
        next_token = None
        while True:
            if next_token is not None:
                token_arg: dict[str, str] = {"nextToken": next_token}
            else:
                token_arg = {}

            async with self.async_conn as client:
                response = await client.get_log_events(
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

                for event in events:
                    await asyncio.sleep(1)
                    yield event

                if next_token != response["nextForwardToken"]:
                    next_token = response["nextForwardToken"]
