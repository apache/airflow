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

from datetime import date, datetime, timedelta, timezone
from functools import cached_property
from typing import TYPE_CHECKING, Any

import watchtower

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.utils import datetime_to_epoch_utc_ms
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models import TaskInstance


def json_serialize_legacy(value: Any) -> str | None:
    """
    JSON serializer replicating legacy watchtower behavior.

    The legacy `watchtower@2.0.1` json serializer function that serialized
    datetime objects as ISO format and all other non-JSON-serializable to `null`.

    :param value: the object to serialize
    :return: string representation of `value` if it is an instance of datetime or `None` otherwise
    """
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    else:
        return None


def json_serialize(value: Any) -> str | None:
    """
    JSON serializer replicating current watchtower behavior.

    This provides customers with an accessible import,
    `airflow.providers.amazon.aws.log.cloudwatch_task_handler.json_serialize`

    :param value: the object to serialize
    :return: string representation of `value`
    """
    return watchtower._json_serialize_default(value)


class CloudwatchTaskHandler(FileTaskHandler, LoggingMixin):
    """
    CloudwatchTaskHandler is a python log handler that handles and reads task instance logs.

    It extends airflow FileTaskHandler and uploads to and reads from Cloudwatch.

    :param base_log_folder: base folder to store logs locally
    :param log_group_arn: ARN of the Cloudwatch log group for remote log storage
        with format ``arn:aws:logs:{region name}:{account id}:log-group:{group name}``
    """

    trigger_should_wrap = True

    def __init__(self, base_log_folder: str, log_group_arn: str, **kwargs):
        super().__init__(base_log_folder)
        split_arn = log_group_arn.split(":")

        self.handler = None
        self.log_group = split_arn[6]
        self.region_name = split_arn[3]
        self.closed = False

    @cached_property
    def hook(self):
        """Returns AwsLogsHook."""
        return AwsLogsHook(
            aws_conn_id=conf.get("logging", "REMOTE_LOG_CONN_ID"),
            region_name=self.region_name,
        )

    def _render_filename(self, ti, try_number):
        # Replace unsupported log group name characters
        return super()._render_filename(ti, try_number).replace(":", "_")

    def set_context(self, ti: TaskInstance, *, identifier: str | None = None):
        super().set_context(ti)
        _json_serialize = conf.getimport(
            "aws", "cloudwatch_task_handler_json_serializer", fallback=None
        )
        self.handler = watchtower.CloudWatchLogHandler(
            log_group_name=self.log_group,
            log_stream_name=self._render_filename(ti, ti.try_number),
            use_queues=not getattr(ti, "is_trigger_log_context", False),
            boto3_client=self.hook.get_conn(),
            json_serialize_default=_json_serialize or json_serialize_legacy,
        )

    def close(self):
        """Close the handler responsible for the upload of the local log file to Cloudwatch."""
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        if self.handler is not None:
            self.handler.close()
        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read(self, task_instance, try_number, metadata=None):
        stream_name = self._render_filename(task_instance, try_number)
        try:
            return (
                f"*** Reading remote log from Cloudwatch log_group: {self.log_group} "
                f"log_stream: {stream_name}.\n"
                f"{self.get_cloudwatch_logs(stream_name=stream_name, task_instance=task_instance)}\n",
                {"end_of_log": True},
            )
        except Exception as e:
            log = (
                f"*** Unable to read remote logs from Cloudwatch (log_group: {self.log_group}, log_stream: "
                f"{stream_name})\n*** {e}\n\n"
            )
            self.log.error(log)
            local_log, metadata = super()._read(task_instance, try_number, metadata)
            log += local_log
            return log, metadata

    def get_cloudwatch_logs(self, stream_name: str, task_instance: TaskInstance) -> str:
        """
        Return all logs from the given log stream.

        :param stream_name: name of the Cloudwatch log stream to get all logs from
        :param task_instance: the task instance to get logs about
        :return: string of all logs from the given log stream
        """
        # If there is an end_date to the task instance, fetch logs until that date + 30 seconds
        # 30 seconds is an arbitrary buffer so that we don't miss any logs that were emitted
        end_time = (
            None
            if task_instance.end_date is None
            else datetime_to_epoch_utc_ms(task_instance.end_date + timedelta(seconds=30))
        )
        events = self.hook.get_log_events(
            log_group=self.log_group,
            log_stream_name=stream_name,
            end_time=end_time,
        )
        return "\n".join(self._event_to_str(event) for event in events)

    def _event_to_str(self, event: dict) -> str:
        event_dt = datetime.fromtimestamp(event["timestamp"] / 1000.0, tz=timezone.utc)
        formatted_event_dt = event_dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        message = event["message"]
        return f"[{formatted_event_dt}] {message}"
