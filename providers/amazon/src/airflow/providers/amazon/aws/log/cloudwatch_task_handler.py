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

import copy
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

import attrs
import watchtower

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.utils import datetime_to_epoch_utc_ms
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    import structlog.typing

    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI
    from airflow.utils.log.file_task_handler import LogMessages, LogSourceInfo


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


@attrs.define(kw_only=True)
class CloudWatchRemoteLogIO(LoggingMixin):  # noqa: D101
    base_log_folder: Path = attrs.field(converter=Path)
    remote_base: str = ""
    delete_local_copy: bool = True

    log_group_arn: str
    log_stream_name: str = ""
    log_group: str = attrs.field(init=False, repr=False)
    region_name: str = attrs.field(init=False, repr=False)

    @log_group.default
    def _(self):
        return self.log_group_arn.split(":")[6]

    @region_name.default
    def _(self):
        return self.log_group_arn.split(":")[3]

    @cached_property
    def hook(self):
        """Returns AwsLogsHook."""
        return AwsLogsHook(
            aws_conn_id=conf.get("logging", "remote_log_conn_id"), region_name=self.region_name
        )

    @cached_property
    def handler(self) -> watchtower.CloudWatchLogHandler:
        _json_serialize = conf.getimport("aws", "cloudwatch_task_handler_json_serializer", fallback=None)
        return watchtower.CloudWatchLogHandler(
            log_group_name=self.log_group,
            log_stream_name=self.log_stream_name,
            use_queues=True,
            boto3_client=self.hook.get_conn(),
            json_serialize_default=_json_serialize or json_serialize_legacy,
        )

    @cached_property
    def processors(self) -> tuple[structlog.typing.Processor, ...]:
        from logging import getLogRecordFactory

        import structlog.stdlib

        logRecordFactory = getLogRecordFactory()
        # The handler MUST be initted here, before the processor is actually used to log anything.
        # Otherwise, logging that occurs during the creation of the handler can create infinite loops.
        _handler = self.handler
        from airflow.sdk.log import relative_path_from_logger

        def proc(logger: structlog.typing.WrappedLogger, method_name: str, event: structlog.typing.EventDict):
            if not logger or not (stream_name := relative_path_from_logger(logger)):
                return event
            # Only init the handler stream_name once. We cannot do it above when we init the handler because
            # we don't yet know the log path at that point.
            if not _handler.log_stream_name:
                _handler.log_stream_name = stream_name.as_posix().replace(":", "_")
            name = event.get("logger_name") or event.get("logger", "")
            level = structlog.stdlib.NAME_TO_LEVEL.get(method_name.lower(), logging.INFO)
            msg = copy.copy(event)
            created = None
            if ts := msg.pop("timestamp", None):
                try:
                    created = datetime.fromisoformat(ts)
                except Exception:
                    pass
            record = logRecordFactory(
                name, level, pathname="", lineno=0, msg=msg, args=(), exc_info=None, func=None, sinfo=None
            )
            if created is not None:
                ct = created.timestamp()
                record.created = ct
                record.msecs = int((ct - int(ct)) * 1000) + 0.0  # Copied from stdlib logging
            _handler.handle(record)
            return event

        return (proc,)

    def close(self):
        self.handler.close()

    def upload(self, path: os.PathLike | str, ti: RuntimeTI):
        # No-op, as we upload via the processor as we go
        # But we need to give the handler time to finish off its business
        self.close()
        return

    def read(self, relative_path, ti: RuntimeTI) -> tuple[LogSourceInfo, LogMessages | None]:
        logs: LogMessages | None = []
        messages = [
            f"Reading remote log from Cloudwatch log_group: {self.log_group} log_stream: {relative_path}"
        ]
        try:
            if AIRFLOW_V_3_0_PLUS:
                from airflow.utils.log.file_task_handler import StructuredLogMessage

                logs = [
                    StructuredLogMessage.model_validate(log)
                    for log in self.get_cloudwatch_logs(relative_path, ti)
                ]
            else:
                logs = [self.get_cloudwatch_logs(relative_path, ti)]  # type: ignore[arg-value]
        except Exception as e:
            logs = None
            messages.append(str(e))

        return messages, logs

    def get_cloudwatch_logs(self, stream_name: str, task_instance: RuntimeTI):
        """
        Return all logs from the given log stream.

        :param stream_name: name of the Cloudwatch log stream to get all logs from
        :param task_instance: the task instance to get logs about
        :return: string of all logs from the given log stream
        """
        stream_name = stream_name.replace(":", "_")
        # If there is an end_date to the task instance, fetch logs until that date + 30 seconds
        # 30 seconds is an arbitrary buffer so that we don't miss any logs that were emitted
        end_time = (
            None
            if (end_date := getattr(task_instance, "end_date", None)) is None
            else datetime_to_epoch_utc_ms(end_date + timedelta(seconds=30))
        )
        events = self.hook.get_log_events(
            log_group=self.log_group,
            log_stream_name=stream_name,
            end_time=end_time,
        )
        if AIRFLOW_V_3_0_PLUS:
            return list(self._event_to_dict(e) for e in events)
        return "\n".join(self._event_to_str(event) for event in events)

    def _event_to_dict(self, event: dict) -> dict:
        event_dt = datetime.fromtimestamp(event["timestamp"] / 1000.0, tz=timezone.utc).isoformat()
        message = event["message"]
        try:
            message = json.loads(message)
            message["timestamp"] = event_dt
            return message
        except Exception:
            return {"timestamp": event_dt, "event": message}

    def _event_to_str(self, event: dict) -> str:
        event_dt = datetime.fromtimestamp(event["timestamp"] / 1000.0, tz=timezone.utc)
        formatted_event_dt = event_dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        message = event["message"]
        return f"[{formatted_event_dt}] {message}"


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

        self.io = CloudWatchRemoteLogIO(
            base_log_folder=base_log_folder,
            log_group_arn=log_group_arn,
        )

    @cached_property
    def hook(self):
        """Returns AwsLogsHook."""
        return AwsLogsHook(
            aws_conn_id=conf.get("logging", "REMOTE_LOG_CONN_ID"), region_name=self.region_name
        )

    def _render_filename(self, ti, try_number):
        # Replace unsupported log group name characters
        return super()._render_filename(ti, try_number).replace(":", "_")

    def set_context(self, ti: TaskInstance, *, identifier: str | None = None):
        super().set_context(ti)
        self.io.log_stream_name = self._render_filename(ti, ti.try_number)

        self.handler = self.io.handler

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

    def _read_remote_logs(
        self, task_instance, try_number, metadata=None
    ) -> tuple[LogSourceInfo, LogMessages]:
        stream_name = self._render_filename(task_instance, try_number)
        messages, logs = self.io.read(stream_name, task_instance)
        return messages, logs or []
