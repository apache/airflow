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
"""Handler that integrates with Stackdriver."""

from __future__ import annotations

import contextlib
import copy
import logging
import os
import shutil
import sys
import warnings
from collections.abc import Collection
from datetime import datetime
from functools import cached_property
from logging import getLogRecordFactory
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlencode

import attrs
from google.cloud import logging as gcp_logging
from google.cloud.logging import Resource
from google.cloud.logging.handlers.transports import BackgroundThreadTransport, Transport
from google.cloud.logging_v2.services.logging_service_v2 import LoggingServiceV2Client
from google.cloud.logging_v2.types import ListLogEntriesRequest, ListLogEntriesResponse

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.utils.credentials_provider import get_credentials_and_project_id
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.log.logging_mixin import LoggingMixin

try:
    from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
except ImportError:
    from airflow.utils.types import NOTSET, ArgNotSet  # type: ignore[attr-defined,no-redef]

if not AIRFLOW_V_3_0_PLUS:
    from airflow.utils.log.trigger_handler import ctx_indiv_trigger

if TYPE_CHECKING:
    import structlog.typing
    from google.auth.credentials import Credentials

    from airflow.models import TaskInstance
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI
    from airflow.utils.log.file_task_handler import LogResponse

DEFAULT_LOGGER_NAME = "airflow"
_GLOBAL_RESOURCE = Resource(type="global", labels={})

# Dedicated logger for handler-internal failures (Cloud Logging unavailable, gRPC errors).
# Routed to the same ``airflow.providers.google.cloud.log.stackdriver_task_handler`` namespace
# so operators see these alongside the rest of the handler's logs.
_logger = logging.getLogger(__name__)

_DEFAULT_SCOPESS = frozenset(
    ["https://www.googleapis.com/auth/logging.read", "https://www.googleapis.com/auth/logging.write"]
)

LABEL_TASK_ID = "task_id"
LABEL_DAG_ID = "dag_id"
LABEL_LOGICAL_DATE = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
LABEL_TRY_NUMBER = "try_number"


@attrs.define(kw_only=True)
class StackdriverRemoteLogIO(LoggingMixin):
    """Remote log IO that streams logs to and reads from Google Cloud Stackdriver Logging."""

    base_log_folder: Path = attrs.field(converter=Path)
    delete_local_copy: bool = True

    gcp_key_path: str | None = None
    scopes: Collection[str] | None = _DEFAULT_SCOPESS
    gcp_log_name: str = DEFAULT_LOGGER_NAME
    transport_type: type[Transport] = BackgroundThreadTransport
    resource: Resource = _GLOBAL_RESOURCE
    labels: dict[str, str] | None = None

    @cached_property
    def credentials_and_project(self) -> tuple[Credentials, str]:
        credentials, project = get_credentials_and_project_id(
            key_path=self.gcp_key_path, scopes=self.scopes, disable_logging=True
        )
        return credentials, project

    @cached_property
    def _client(self) -> gcp_logging.Client:
        """The Cloud Library API client."""
        credentials, project = self.credentials_and_project
        return gcp_logging.Client(
            credentials=credentials,
            project=project,
            client_info=CLIENT_INFO,
        )

    @cached_property
    def _logging_service_client(self) -> LoggingServiceV2Client:
        """The Cloud logging service v2 client."""
        credentials, _ = self.credentials_and_project
        return LoggingServiceV2Client(
            credentials=credentials,
            client_info=CLIENT_INFO,
        )

    @cached_property
    def transport(self) -> Transport:
        """Object responsible for sending data to Stackdriver."""
        return self.transport_type(self._client, self.gcp_log_name)

    @cached_property
    def processors(self) -> tuple[structlog.typing.Processor, ...]:
        import structlog.stdlib

        from airflow.sdk.log import relative_path_from_logger

        log_record_factory = getLogRecordFactory()
        _transport = self.transport

        def proc(
            logger: structlog.typing.WrappedLogger,
            method_name: str,
            event: structlog.typing.EventDict,
        ):
            if not logger or not relative_path_from_logger(logger):
                return event

            name = event.get("logger_name") or event.get("logger", "")
            level = structlog.stdlib.NAME_TO_LEVEL.get(method_name.lower(), logging.INFO)
            msg = copy.copy(event)
            created = None
            if ts := msg.pop("timestamp", None):
                with contextlib.suppress(Exception):
                    created = datetime.fromisoformat(ts)
            record = log_record_factory(
                name,
                level,
                pathname="",
                lineno=0,
                msg=msg,
                args=(),
                exc_info=None,
                func=None,
                sinfo=None,
            )
            if created is not None:
                ct = created.timestamp()
                record.created = ct
                record.msecs = int((ct - int(ct)) * 1000) + 0.0

            ti = getattr(record, "task_instance", None)
            labels: dict[str, str] = {}
            if self.labels:
                labels.update(self.labels)
            if ti:
                labels.update(_task_instance_to_labels(ti))
            else:
                if dag_id := event.get("dag_id"):
                    labels[LABEL_DAG_ID] = str(dag_id)
                if task_id := event.get("task_id"):
                    labels[LABEL_TASK_ID] = str(task_id)
                if run_id := event.get("run_id"):
                    labels["run_id"] = str(run_id)
                if try_number := event.get("try_number"):
                    labels[LABEL_TRY_NUMBER] = str(try_number)
                if map_index := event.get("map_index"):
                    labels["map_index"] = str(map_index)

            _transport.send(record, str(msg.get("event", "")), resource=self.resource, labels=labels)
            return event

        return (proc,)

    def upload(self, path: os.PathLike | str, ti: RuntimeTI | None = None) -> None:
        """Flush the transport and optionally delete local log files."""
        self.transport.flush()
        if self.delete_local_copy:
            base = self.base_log_folder.resolve()
            raw = Path(path)
            local_path = (raw if raw.is_absolute() else base / raw).resolve()
            try:
                local_path.relative_to(base)
            except ValueError:
                self.log.warning(
                    "Skipping deletion: path %s is outside base_log_folder %s",
                    local_path,
                    base,
                )
                return
            parent = local_path.parent
            if parent.exists():
                shutil.rmtree(parent, ignore_errors=True)

    def read(self, relative_path: str, ti: RuntimeTI) -> LogResponse:
        """Read logs from Stackdriver Logging using task instance labels."""
        ti_labels = _task_instance_to_labels(ti)
        log_filter = self.prepare_log_filter(ti_labels)
        messages, end_of_log, _ = self.read_logs(log_filter, next_page_token=None, all_pages=True)
        return [f"Reading remote log from Stackdriver for {relative_path}"], [messages] if messages else []

    def prepare_log_filter(self, ti_labels: dict[str, str]) -> str:
        def escape_label_key(key: str) -> str:
            return f'"{key}"' if "." in key else key

        def escape_label_value(value: str) -> str:
            escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped_value}"'

        _, project = self.credentials_and_project
        log_filters = [
            f"resource.type={escape_label_value(self.resource.type)}",
            f'logName="projects/{project}/logs/{self.gcp_log_name}"',
        ]

        for key, value in self.resource.labels.items():
            log_filters.append(f"resource.labels.{escape_label_key(key)}={escape_label_value(value)}")

        for key, value in ti_labels.items():
            log_filters.append(f"labels.{escape_label_key(key)}={escape_label_value(value)}")
        return "\n".join(log_filters)

    def read_logs(
        self, log_filter: str, next_page_token: str | None, all_pages: bool
    ) -> tuple[str, bool, str | None]:
        messages = []
        new_messages, next_page_token = self._read_single_logs_page(
            log_filter=log_filter,
            page_token=next_page_token,
        )
        messages.append(new_messages)
        if all_pages:
            while next_page_token:
                new_messages, next_page_token = self._read_single_logs_page(
                    log_filter=log_filter, page_token=next_page_token
                )
                messages.append(new_messages)

            end_of_log = True
            next_page_token = None
        else:
            end_of_log = not bool(next_page_token)
        return "\n".join(messages), end_of_log, next_page_token

    def _read_single_logs_page(self, log_filter: str, page_token: str | None = None) -> tuple[str, str]:
        _, project = self.credentials_and_project
        request = ListLogEntriesRequest(
            resource_names=[f"projects/{project}"],
            filter=log_filter,
            page_token=page_token,
            order_by="timestamp asc",
            page_size=1000,
        )
        response = self._logging_service_client.list_log_entries(request=request)
        page: ListLogEntriesResponse = next(response.pages)
        messages: list[str] = []
        for entry in page.entries:
            if "message" in (entry.json_payload or {}):
                messages.append(entry.json_payload["message"])  # type: ignore
            elif entry.text_payload:
                messages.append(entry.text_payload)
        return "\n".join(messages), page.next_page_token


def _task_instance_to_labels(ti) -> dict[str, str]:
    """Convert a task instance to Stackdriver labels."""
    return {
        LABEL_TASK_ID: ti.task_id,
        LABEL_DAG_ID: ti.dag_id,
        LABEL_LOGICAL_DATE: str(ti.logical_date.isoformat())
        if AIRFLOW_V_3_0_PLUS
        else str(ti.execution_date.isoformat()),
        LABEL_TRY_NUMBER: str(ti.try_number),
    }


class StackdriverTaskHandler(logging.Handler):
    """
    Handler that directly makes Stackdriver logging API calls.

    This is a Python standard ``logging`` handler using that can be used to
    route Python standard logging messages directly to the Stackdriver
    Logging API.

    It can also be used to save logs for executing tasks. To do this, you should set as a handler with
    the name "tasks". In this case, it will also be used to read the log for display in Web UI.

    This handler supports both an asynchronous and synchronous transport.

    :param gcp_key_path: Path to Google Cloud Credential JSON file.
        If omitted, authorization based on `the Application Default Credentials
        <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__ will
        be used.
    :param scopes: OAuth scopes for the credentials,
    :param name: the name of the custom log in Stackdriver Logging. Defaults
        to 'airflow'. The name of the Python logger will be represented
        in the ``python_logger`` field.
    :param transport: Class for creating new transport objects. It should
        extend from the base :class:`google.cloud.logging.handlers.Transport` type and
        implement :meth`google.cloud.logging.handlers.Transport.send`. Defaults to
        :class:`google.cloud.logging.handlers.BackgroundThreadTransport`. The other
        option is :class:`google.cloud.logging.handlers.SyncTransport`.
    :param resource: (Optional) Monitored resource of the entry, defaults
                     to the global resource type.
    :param labels: (Optional) Mapping of labels for the entry.
    """

    # Re-export module-level constants for back-compat with external code reading them off the class
    LABEL_TASK_ID = LABEL_TASK_ID
    LABEL_DAG_ID = LABEL_DAG_ID
    LABEL_LOGICAL_DATE = LABEL_LOGICAL_DATE
    LABEL_TRY_NUMBER = LABEL_TRY_NUMBER
    LOG_VIEWER_BASE_URL = "https://console.cloud.google.com/logs/viewer"
    LOG_NAME = "Google Stackdriver"

    trigger_supported = True
    trigger_should_queue = False
    trigger_should_wrap = False
    trigger_send_end_marker = False

    def __init__(
        self,
        gcp_key_path: str | None = None,
        scopes: Collection[str] | None = _DEFAULT_SCOPESS,
        name: str | ArgNotSet = NOTSET,
        transport: type[Transport] = BackgroundThreadTransport,
        resource: Resource = _GLOBAL_RESOURCE,
        labels: dict[str, str] | None = None,
        gcp_log_name: str = DEFAULT_LOGGER_NAME,
    ):
        if name is not NOTSET:
            warnings.warn(
                "Param `name` is deprecated and will be removed in a future release. "
                "Please use `gcp_log_name` instead. Planned removal date: October 5, 2026.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            gcp_log_name = str(name)

        super().__init__()
        self.io = StackdriverRemoteLogIO(
            base_log_folder=Path("."),
            gcp_key_path=gcp_key_path,
            scopes=scopes,
            gcp_log_name=gcp_log_name,
            transport_type=transport,
            resource=resource,
            labels=labels,
        )
        self.labels: dict[str, str] | None = labels
        self.resource: Resource = resource
        self.task_instance_labels: dict[str, str] | None = {}
        self.task_instance_hostname = "default-hostname"

    def _get_labels(self, task_instance=None):
        if task_instance:
            ti_labels = _task_instance_to_labels(task_instance)
        else:
            ti_labels = self.task_instance_labels
        labels: dict[str, str] | None
        if self.labels and ti_labels:
            labels = {}
            labels.update(self.labels)
            labels.update(ti_labels)
        elif self.labels:
            labels = self.labels
        elif ti_labels:
            labels = ti_labels
        else:
            labels = None
        return labels or {}

    def emit(self, record: logging.LogRecord) -> None:
        """
        Actually log the specified logging record.

        :param record: The record to be logged.
        """
        message = self.format(record)
        ti = None
        if not AIRFLOW_V_3_0_PLUS and getattr(record, ctx_indiv_trigger.name, None):
            ti = getattr(record, "task_instance", None)  # trigger context
        labels = self._get_labels(ti)
        self.io.transport.send(record, message, resource=self.resource, labels=labels)

    def set_context(self, task_instance: TaskInstance) -> None:
        """
        Configure the logger to add information with information about the current task.

        :param task_instance: Currently executed task
        """
        self.task_instance_labels = _task_instance_to_labels(task_instance)
        self.task_instance_hostname = task_instance.hostname or "default-hostname"

    def read(
        self, task_instance: TaskInstance, try_number: int | None = None, metadata: dict | None = None
    ) -> tuple[list[tuple[tuple[str, str]]], list[dict[str, str | bool]]]:
        """
        Read logs of given task instance from Stackdriver logging.

        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from. If None
           it returns all logs
        :param metadata: log metadata. It is used for steaming log reading and auto-tailing.
        :return: a tuple of (
            list of (one element tuple with two element tuple - hostname and logs)
            and list of metadata)
        """
        if try_number is not None and try_number < 1:
            logs = f"Error fetching the logs. Try number {try_number} is invalid."
            return [((self.task_instance_hostname, logs),)], [{"end_of_log": "true"}]

        if not metadata:
            metadata = {}

        ti_labels = _task_instance_to_labels(task_instance)

        if try_number is not None:
            ti_labels[LABEL_TRY_NUMBER] = str(try_number)
        else:
            del ti_labels[LABEL_TRY_NUMBER]

        log_filter = self.io.prepare_log_filter(ti_labels)
        next_page_token = metadata.get("next_page_token", None)
        all_pages = "download_logs" in metadata and metadata["download_logs"]

        try:
            messages, end_of_log, next_page_token = self.io.read_logs(log_filter, next_page_token, all_pages)
        except Exception:
            # Cloud Logging unavailable / IAM glitch / gRPC error. Without a guard, the
            # exception used to propagate up as HTTP 500 from the log viewer. Degrade
            # gracefully instead: surface a short user-facing message, mark the read
            # complete (no spinning retry), and log the full traceback to the handler's
            # own logger for the operator.
            _logger.exception("Failed to read logs from Cloud Logging for filter %s", log_filter)
            return (
                [((self.task_instance_hostname, "Cloud Logging is currently unavailable."),)],
                [{"end_of_log": True}],
            )

        new_metadata: dict[str, str | bool] = {"end_of_log": end_of_log}

        if next_page_token:
            new_metadata["next_page_token"] = next_page_token

        return [((self.task_instance_hostname, messages),)], [new_metadata]

    @classmethod
    def _task_instance_to_labels(cls, ti: TaskInstance) -> dict[str, str]:
        return {
            cls.LABEL_TASK_ID: ti.task_id,
            cls.LABEL_DAG_ID: ti.dag_id,
            cls.LABEL_LOGICAL_DATE: str(ti.logical_date.isoformat())
            if AIRFLOW_V_3_0_PLUS
            else str(ti.execution_date.isoformat()),
            cls.LABEL_TRY_NUMBER: str(ti.try_number),
        }

    @property
    def log_name(self):
        """Return log name."""
        return self.LOG_NAME

    @cached_property
    def _resource_path(self):
        segments = [self.resource.type]

        for key, value in self.resource.labels:
            segments += [key]
            segments += [value]

        return "/".join(segments)

    def get_external_log_url(self, task_instance: TaskInstance, try_number: int) -> str:
        """
        Create an address for an external log collecting service.

        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from
        :return: URL to the external log collection service
        """
        _, project_id = self.io.credentials_and_project

        ti_labels = _task_instance_to_labels(task_instance)
        ti_labels[LABEL_TRY_NUMBER] = str(try_number)

        log_filter = self.io.prepare_log_filter(ti_labels)

        url_query_string = {
            "project": project_id,
            "interval": "NO_LIMIT",
            "resource": self._resource_path,
            "advancedFilter": log_filter,
        }

        url = f"{self.LOG_VIEWER_BASE_URL}?{urlencode(url_query_string)}"
        return url

    def close(self) -> None:
        # ``flush()`` is best-effort during shutdown — if Cloud Logging is unavailable or
        # the transport raises, that's not a reason to break the rest of the handler's
        # shutdown chain (and the stdlib logging machinery does not handle exceptions
        # from ``Handler.close()`` gracefully). Print to stderr as last resort since
        # logging itself may be shutting down.
        try:
            self.io.transport.flush()
        except Exception as exc:
            print(
                f"StackdriverTaskHandler.close: transport flush failed: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
