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

import time
from datetime import datetime, timedelta, timezone
from threading import Event, Thread
from typing import TYPE_CHECKING, Generator

from botocore.exceptions import ClientError, ConnectionClosedError

from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook

if TYPE_CHECKING:
    from logging import Logger


class AwsTaskLogFetcher(Thread):
    """Fetch Cloudwatch log events with specific interval and send the log events to the logger.info."""

    def __init__(
        self,
        *,
        log_group: str,
        log_stream_name: str,
        fetch_interval: timedelta,
        logger: Logger,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
    ):
        super().__init__()
        self._event = Event()

        self.fetch_interval = fetch_interval

        self.logger = logger
        self.log_group = log_group
        self.log_stream_name = log_stream_name

        self.hook = AwsLogsHook(aws_conn_id=aws_conn_id, region_name=region_name)

    def run(self) -> None:
        continuation_token = AwsLogsHook.ContinuationToken()
        while not self.is_stopped():
            time.sleep(self.fetch_interval.total_seconds())
            log_events = self._get_log_events(continuation_token)
            prev_timestamp_event = None
            for log_event in log_events:
                current_timestamp_event = datetime.fromtimestamp(
                    log_event["timestamp"] / 1000.0, tz=timezone.utc
                )
                if current_timestamp_event == prev_timestamp_event:
                    # When multiple events have the same timestamp, somehow, only one event is logged
                    # As a consequence, some logs are missed in the log group (in case they have the same
                    # timestamp)
                    # When a slight delay is added before logging the event, that solves the issue
                    # See https://github.com/apache/airflow/issues/40875
                    time.sleep(0.1)
                self.logger.info(self.event_to_str(log_event))
                prev_timestamp_event = current_timestamp_event

    def _get_log_events(self, skip_token: AwsLogsHook.ContinuationToken | None = None) -> Generator:
        if skip_token is None:
            skip_token = AwsLogsHook.ContinuationToken()
        try:
            yield from self.hook.get_log_events(
                self.log_group, self.log_stream_name, continuation_token=skip_token
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceNotFoundException":
                self.logger.warning("Error on retrieving Cloudwatch log events", error)
            else:
                self.logger.info(
                    "Cannot find log stream yet, it can take a couple of seconds to show up. "
                    "If this error persists, check that the log group and stream are correct: "
                    "group: %s\tstream: %s",
                    self.log_group,
                    self.log_stream_name,
                )
            yield from ()
        except ConnectionClosedError as error:
            self.logger.warning("ConnectionClosedError on retrieving Cloudwatch log events", error)
            yield from ()

    @staticmethod
    def event_to_str(event: dict) -> str:
        event_dt = datetime.fromtimestamp(event["timestamp"] / 1000.0, tz=timezone.utc)
        formatted_event_dt = event_dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        message = event["message"]
        return f"[{formatted_event_dt}] {message}"

    def get_last_log_messages(self, number_messages) -> list:
        """
        Get the last logs messages in one single request.

         NOTE: some restrictions apply:
         - if logs are too old, the response will be empty
         - the max number of messages we can retrieve is constrained by cloudwatch limits (10,000).
        """
        response = self.hook.conn.get_log_events(
            logGroupName=self.log_group,
            logStreamName=self.log_stream_name,
            startFromHead=False,
            limit=number_messages,
        )
        return [log["message"] for log in response["events"]]

    def get_last_log_message(self) -> str | None:
        try:
            return self.get_last_log_messages(1)[0]
        except IndexError:
            return None

    def is_stopped(self) -> bool:
        return self._event.is_set()

    def stop(self):
        self._event.set()
