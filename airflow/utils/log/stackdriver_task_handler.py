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
Handler that integrates with Stackdriver
"""
import logging
from typing import Collection, Dict, List, Optional, Tuple, Type

from cached_property import cached_property
from google.api_core.gapic_v1.client_info import ClientInfo
from google.cloud import logging as gcp_logging
from google.cloud.logging.handlers.transports import BackgroundThreadTransport, Transport
from google.cloud.logging.resource import Resource

from airflow import version
from airflow.models import TaskInstance
from airflow.providers.google.cloud.utils.credentials_provider import get_credentials_and_project_id

DEFAULT_LOGGER_NAME = "airflow"
_GLOBAL_RESOURCE = Resource(type="global", labels={})

_DEFAULT_SCOPESS = frozenset([
    "https://www.googleapis.com/auth/logging.read",
    "https://www.googleapis.com/auth/logging.write"
])


class StackdriverTaskHandler(logging.Handler):
    """Handler that directly makes Stackdriver logging API calls.

    This is a Python standard ``logging`` handler using that can be used to
    route Python standard logging messages directly to the Stackdriver
    Logging API.

    It can also be used to save logs for executing tasks. To do this, you should set as a handler with
    the name "tasks". In this case, it will also be used to read the log for display in Web UI.

    This handler supports both an asynchronous and synchronous transport.


    :param gcp_key_path: Path to GCP Credential JSON file.
        If ommited, authorization based on `the Application Default Credentials
        <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__ will
        be used.
    :type gcp_key_path: str
    :param scopes: OAuth scopes for the credentials,
    :type scopes: Sequence[str]
    :param name: the name of the custom log in Stackdriver Logging. Defaults
        to 'airflow'. The name of the Python logger will be represented
         in the ``python_logger`` field.
    :type name: str
    :param transport: Class for creating new transport objects. It should
        extend from the base :class:`google.cloud.logging.handlers.Transport` type and
        implement :meth`google.cloud.logging.handlers.Transport.send`. Defaults to
        :class:`google.cloud.logging.handlers.BackgroundThreadTransport`. The other
        option is :class:`google.cloud.logging.handlers.SyncTransport`.
    :type transport: :class:`type`
    :param resource: (Optional) Monitored resource of the entry, defaults
                     to the global resource type.
    :type resource: :class:`~google.cloud.logging.resource.Resource`
    :param labels: (Optional) Mapping of labels for the entry.
    :type labels: dict
    """

    LABEL_TASK_ID = "task_id"
    LABEL_DAG_ID = "dag_id"
    LABEL_EXECUTION_DATE = "execution_date"
    LABEL_TRY_NUMBER = "try_number"

    def __init__(
        self,
        gcp_key_path: Optional[str] = None,
        # See: https://github.com/PyCQA/pylint/issues/2377
        scopes: Optional[Collection[str]] = _DEFAULT_SCOPESS,  # pylint: disable=unsubscriptable-object
        name: str = DEFAULT_LOGGER_NAME,
        transport: Type[Transport] = BackgroundThreadTransport,
        resource: Resource = _GLOBAL_RESOURCE,
        labels: Optional[Dict[str, str]] = None,
    ):
        super().__init__()
        self.gcp_key_path: Optional[str] = gcp_key_path
        # See: https://github.com/PyCQA/pylint/issues/2377
        self.scopes: Optional[Collection[str]] = scopes  # pylint: disable=unsubscriptable-object
        self.name: str = name
        self.transport_type: Type[Transport] = transport
        self.resource: Resource = resource
        self.labels: Optional[Dict[str, str]] = labels
        self.task_instance_labels: Optional[Dict[str, str]] = {}

    @cached_property
    def _client(self) -> gcp_logging.Client:
        """Google Cloud Library API client"""
        credentials = get_credentials_and_project_id(
            key_path=self.gcp_key_path,
            scopes=self.scopes,
        )
        client = gcp_logging.Client(
            credentials=credentials,
            client_info=ClientInfo(client_library_version='airflow_v' + version.version)
        )
        return client

    @cached_property
    def _transport(self) -> Transport:
        """Object responsible for sending data to Stackdriver"""
        return self.transport_type(self._client, self.name)

    def emit(self, record: logging.LogRecord) -> None:
        """Actually log the specified logging record.

        :param record: The record to be logged.
        :type record: logging.LogRecord
        """
        message = self.format(record)
        labels: Optional[Dict[str, str]]
        if self.labels and self.task_instance_labels:
            labels = {}
            labels.update(self.labels)
            labels.update(self.task_instance_labels)
        elif self.labels:
            labels = self.labels
        elif self.task_instance_labels:
            labels = self.task_instance_labels
        else:
            labels = None
        self._transport.send(record, message, resource=self.resource, labels=labels)

    def set_context(self, task_instance: TaskInstance) -> None:
        """
        Configures the logger to add information with information about the current task

        :param task_instance: Currently executed task
        :type task_instance: TaskInstance
        """
        self.task_instance_labels = self._task_instance_to_labels(task_instance)

    def read(
        self, task_instance: TaskInstance, try_number: Optional[int] = None, metadata: Optional[Dict] = None
    ) -> Tuple[List[str], List[Dict]]:
        """
        Read logs of given task instance from Stackdriver logging.

        :param task_instance: task instance object
        :type: task_instance: TaskInstance
        :param try_number: task instance try_number to read logs from. If None
           it returns all logs
        :type try_number: Optional[int]
        :param metadata: log metadata. It is used for steaming log reading and auto-tailing.
        :type metadata: Dict
        :return: a tuple of list of logs and list of metadata
        :rtype: Tuple[List[str], List[Dict]]
        """
        if try_number is not None and try_number < 1:
            logs = ["Error fetching the logs. Try number {} is invalid.".format(try_number)]
            return logs, [{"end_of_log": "true"}]

        if not metadata:
            metadata = {}

        ti_labels = self._task_instance_to_labels(task_instance)

        if try_number is not None:
            ti_labels[self.LABEL_TRY_NUMBER] = str(try_number)
        else:
            del ti_labels[self.LABEL_TRY_NUMBER]

        log_filter = self._prepare_log_filter(ti_labels)
        next_page_token = metadata.get("next_page_token", None)
        all_pages = 'download_logs' in metadata and metadata['download_logs']

        messages, end_of_log, next_page_token = self._read_logs(log_filter, next_page_token, all_pages)

        new_metadata = {"end_of_log": end_of_log}

        if next_page_token:
            new_metadata['next_page_token'] = next_page_token

        return [messages], [new_metadata]

    def _prepare_log_filter(self, ti_labels: Dict[str, str]) -> str:
        """
        Prepares the filter that chooses which log entries to fetch.

        More information:
        https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/list#body.request_body.FIELDS.filter
        https://cloud.google.com/logging/docs/view/advanced-queries

        :param ti_labels: Task Instance's labels that will be used to search for logs
        :type: Dict[str, str]
        :return: logs filter
        """
        def escape_label_key(key: str) -> str:
            return f'"{key}"' if "." in key else key

        def escale_label_value(value: str) -> str:
            escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped_value}"'

        log_filters = [
            f'resource.type={escale_label_value(self.resource.type)}',
            f'logName="projects/{self._client.project}/logs/{self.name}"'
        ]

        for key, value in self.resource.labels.items():
            log_filters.append(f'resource.labels.{escape_label_key(key)}={escale_label_value(value)}')

        for key, value in ti_labels.items():
            log_filters.append(f'labels.{escape_label_key(key)}={escale_label_value(value)}')
        return "\n".join(log_filters)

    def _read_logs(
        self,
        log_filter: str,
        next_page_token: Optional[str],
        all_pages: bool
    ) -> Tuple[str, bool, Optional[str]]:
        """
        Sends requests to the Stackdriver service and downloads logs.

        :param log_filter: Filter specifying the logs to be downloaded.
        :type log_filter: str
        :param next_page_token: The token of the page from which the log download will start.
            If None is passed, it will start from the first page.
        :param all_pages: If True is passed, all subpages will be downloaded. Otherwise, only the first
            page will be downloaded
        :return: A token that contains the following items:
            * string with logs
            * Boolean value describing whether there are more logs,
            * token of the next page
        :rtype: Tuple[str, bool, str]
        """
        messages = []
        new_messages, next_page_token = self._read_single_logs_page(
            log_filter=log_filter,
            page_token=next_page_token,
        )
        messages.append(new_messages)
        if all_pages:
            while next_page_token:
                new_messages, next_page_token = self._read_single_logs_page(
                    log_filter=log_filter,
                    page_token=next_page_token
                )
                messages.append(new_messages)

            end_of_log = True
            next_page_token = None
        else:
            end_of_log = not bool(next_page_token)
        return "\n".join(messages), end_of_log, next_page_token

    def _read_single_logs_page(self, log_filter: str, page_token: Optional[str] = None) -> Tuple[str, str]:
        """
        Sends requests to the Stackdriver service and downloads single pages with logs.

        :param log_filter: Filter specifying the logs to be downloaded.
        :type log_filter: str
        :param page_token: The token of the page to be downloaded. If None is passed, the first page will be
            downloaded.
        :type page_token: str
        :return: Downloaded logs and next page token
        :rtype: Tuple[str, str]
        """
        entries = self._client.list_entries(filter_=log_filter, page_token=page_token)
        page = next(entries.pages)
        next_page_token = entries.next_page_token
        messages = []
        for entry in page:
            if "message" in entry.payload:
                messages.append(entry.payload["message"])

        return "\n".join(messages), next_page_token

    @classmethod
    def _task_instance_to_labels(cls, ti: TaskInstance) -> Dict[str, str]:
        return {
            cls.LABEL_TASK_ID: ti.task_id,
            cls.LABEL_DAG_ID: ti.dag_id,
            cls.LABEL_EXECUTION_DATE: str(ti.execution_date.isoformat()),
            cls.LABEL_TRY_NUMBER: str(ti.try_number),
        }
