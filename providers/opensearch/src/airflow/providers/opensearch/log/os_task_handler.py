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

import contextlib
import logging
import sys
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime
from operator import attrgetter
from typing import TYPE_CHECKING, Any, Literal, cast
from urllib.parse import urlparse

import pendulum
from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError

from airflow.configuration import conf
from airflow.models import DagRun
from airflow.providers.common.compat.module_loading import import_string
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.opensearch.log.os_json_formatter import OpensearchJSONFormatter
from airflow.providers.opensearch.log.os_response import Hit, OpensearchResponse
from airflow.providers.opensearch.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils import timezone
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import ExternalLoggingMixin, LoggingMixin
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
    from airflow.utils.log.file_task_handler import LogMetadata

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.log.file_task_handler import StructuredLogMessage

    OsLogMsgType = list[StructuredLogMessage] | str
else:
    OsLogMsgType = list[tuple[str, str]]  # type: ignore[assignment,misc]


USE_PER_RUN_LOG_ID = hasattr(DagRun, "get_log_template")
LOG_LINE_DEFAULTS = {"exc_text": "", "stack_info": ""}
TASK_LOG_FIELDS = ["timestamp", "event", "level", "chan", "logger"]


def getattr_nested(obj, item, default):
    """
    Get item from obj but return default if not found.

    E.g. calling ``getattr_nested(a, 'b.c', "NA")`` will return
    ``a.b.c`` if such a value exists, and "NA" otherwise.

    :meta private:
    """
    try:
        return attrgetter(item)(obj)
    except AttributeError:
        return default


def _ensure_ti(ti: TaskInstanceKey | TaskInstance, session) -> TaskInstance:
    """
    Given TI | TIKey, return a TI object.

    Will raise exception if no TI is found in the database.
    """
    from airflow.models.taskinstance import TaskInstance, TaskInstanceKey

    if not isinstance(ti, TaskInstanceKey):
        return ti
    val = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.task_id == ti.task_id,
            TaskInstance.dag_id == ti.dag_id,
            TaskInstance.run_id == ti.run_id,
            TaskInstance.map_index == ti.map_index,
        )
        .one_or_none()
    )
    if isinstance(val, TaskInstance):
        val.try_number = ti.try_number
        return val
    raise AirflowException(f"Could not find TaskInstance for {ti}")


def get_os_kwargs_from_config() -> dict[str, Any]:
    open_search_config = conf.getsection("opensearch_configs")
    kwargs_dict = {key: value for key, value in open_search_config.items()} if open_search_config else {}

    return kwargs_dict


class OpensearchTaskHandler(FileTaskHandler, ExternalLoggingMixin, LoggingMixin):
    """
    OpensearchTaskHandler is a Python log handler that reads and writes logs to OpenSearch.

    Like the ElasticsearchTaskHandler, Airflow itself does not handle the indexing of logs.
    Instead, logs are flushed to local files, and additional software (e.g., Filebeat, Logstash)
    may be required to ship logs to OpenSearch. This handler then enables fetching and displaying
    logs from OpenSearch.

    To efficiently query and sort Elasticsearch results, this handler assumes each
    log message has a field `log_id` consists of ti primary keys:
    `log_id = {dag_id}-{task_id}-{logical_date}-{try_number}`
    Log messages with specific log_id are sorted based on `offset`,
    which is a unique integer indicates log message's order.
    Timestamps here are unreliable because multiple log messages
    might have the same timestamp.

    :param base_log_folder: Base folder to store logs locally.
    :param end_of_log_mark: A marker string to signify the end of logs.
    :param write_stdout: Whether to also write logs to stdout.
    :param json_format: Whether to format logs as JSON.
    :param json_fields: Comma-separated list of fields to include in the JSON log output.
    :param host: OpenSearch host name.
    :param port: OpenSearch port.
    :param username: Username for OpenSearch authentication.
    :param password: Password for OpenSearch authentication.
    :param host_field: The field name for the host in the logs (default is "host").
    :param offset_field: The field name for the log offset (default is "offset").
    :param index_patterns: Index pattern or template for storing logs.
    :param index_patterns_callable: Callable that dynamically generates index patterns based on context.
    :param os_kwargs: Additional OpenSearch client options. This can be set to "default_os_kwargs" to
                      load the default configuration from Airflow's settings.
    """

    PAGE = 0
    MAX_LINE_PER_PAGE = 1000
    LOG_NAME = "Opensearch"

    trigger_should_wrap = True

    def __init__(
        self,
        base_log_folder: str,
        end_of_log_mark: str,
        write_stdout: bool,
        json_format: bool,
        json_fields: str,
        host: str,
        port: int,
        username: str,
        password: str,
        host_field: str = "host",
        offset_field: str = "offset",
        index_patterns: str = conf.get("opensearch", "index_patterns", fallback="_all"),
        index_patterns_callable: str = conf.get("opensearch", "index_patterns_callable", fallback=""),
        os_kwargs: dict | None | Literal["default_os_kwargs"] = "default_os_kwargs",
        max_bytes: int = 0,
        backup_count: int = 0,
        delay: bool = False,
    ) -> None:
        os_kwargs = os_kwargs or {}
        if os_kwargs == "default_os_kwargs":
            os_kwargs = get_os_kwargs_from_config()
        # support log file size handling of FileTaskHandler
        super().__init__(
            base_log_folder=base_log_folder, max_bytes=max_bytes, backup_count=backup_count, delay=delay
        )
        self.closed = False
        self.mark_end_on_close = True
        self.end_of_log_mark = end_of_log_mark.strip()
        self.write_stdout = write_stdout
        self.json_format = json_format
        self.json_fields = [label.strip() for label in json_fields.split(",")]
        self.host = self.format_url(host)
        self.host_field = host_field
        self.offset_field = offset_field
        self.index_patterns = index_patterns
        self.index_patterns_callable = index_patterns_callable
        self.context_set = False
        self.client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=(username, password),
            **os_kwargs,
        )
        self.formatter: logging.Formatter
        self.handler: logging.FileHandler | logging.StreamHandler
        self._doc_type_map: dict[Any, Any] = {}
        self._doc_type: list[Any] = []

    def set_context(self, ti: TaskInstance, *, identifier: str | None = None) -> None:
        """
        Provide task_instance context to airflow task handler.

        :param ti: task instance object
        :param identifier: if set, identifies the Airflow component which is relaying logs from
            exceptional scenarios related to the task instance
        """
        is_trigger_log_context = getattr(ti, "is_trigger_log_context", None)
        is_ti_raw = getattr(ti, "raw", None)
        self.mark_end_on_close = not is_ti_raw and not is_trigger_log_context
        date_key = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        if self.json_format:
            self.formatter = OpensearchJSONFormatter(
                fmt=self.formatter._fmt,
                json_fields=[*self.json_fields, self.offset_field],
                extras={
                    "dag_id": str(ti.dag_id),
                    "task_id": str(ti.task_id),
                    date_key: (
                        self._clean_date(ti.logical_date)
                        if AIRFLOW_V_3_0_PLUS
                        else self._clean_date(ti.execution_date)
                    ),
                    "try_number": str(ti.try_number),
                    "log_id": self._render_log_id(ti, ti.try_number),
                },
            )

        if self.write_stdout:
            if self.context_set:
                # We don't want to re-set up the handler if this logger has
                # already been initialized
                return

            self.handler = logging.StreamHandler(stream=sys.__stdout__)
            self.handler.setLevel(self.level)
            self.handler.setFormatter(self.formatter)
        else:
            super().set_context(ti, identifier=identifier)
        self.context_set = True

    def emit(self, record):
        if self.handler:
            setattr(record, self.offset_field, int(time.time() * (10**9)))
            self.handler.emit(record)

    def close(self) -> None:
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        if not self.mark_end_on_close:
            # when we're closing due to task deferral, don't mark end of log
            self.closed = True
            return

        # Case which context of the handler was not set.
        if self.handler is None:
            self.closed = True
            return

        # Reopen the file stream, because FileHandler.close() would be called
        # first in logging.shutdown() and the stream in it would be set to None.
        if self.handler.stream is None or self.handler.stream.closed:
            self.handler.stream = self.handler._open()  # type: ignore[union-attr]

        # Mark the end of file using end of log mark,
        # so we know where to stop while auto-tailing.
        self.emit(logging.makeLogRecord({"msg": self.end_of_log_mark}))

        if self.write_stdout:
            self.handler.close()
            sys.stdout = sys.__stdout__

        super().close()

        self.closed = True

    def _read_grouped_logs(self):
        return True

    @staticmethod
    def _clean_date(value: datetime | None) -> str:
        """
        Clean up a date value so that it is safe to query in elasticsearch by removing reserved characters.

        https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
        """
        if value is None:
            return ""
        return value.strftime("%Y_%m_%dT%H_%M_%S_%f")

    def _render_log_id(self, ti: TaskInstance | TaskInstanceKey, try_number: int) -> str:
        from airflow.models.taskinstance import TaskInstanceKey

        with create_session() as session:
            if isinstance(ti, TaskInstanceKey):
                ti = _ensure_ti(ti, session)
            dag_run = ti.get_dagrun(session=session)
            if USE_PER_RUN_LOG_ID:
                log_id_template = dag_run.get_log_template(session=session).elasticsearch_id

        if self.json_format:
            data_interval_start = self._clean_date(dag_run.data_interval_start)
            data_interval_end = self._clean_date(dag_run.data_interval_end)
            logical_date = self._clean_date(dag_run.logical_date)
        else:
            data_interval_start = (
                dag_run.data_interval_start.isoformat() if dag_run.data_interval_start else ""
            )
            data_interval_end = dag_run.data_interval_end.isoformat() if dag_run.data_interval_end else ""
            logical_date = dag_run.logical_date.isoformat() if dag_run.logical_date else ""
        return log_id_template.format(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=getattr(ti, "run_id", ""),
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            logical_date=logical_date,
            execution_date=logical_date,
            try_number=try_number,
            map_index=getattr(ti, "map_index", ""),
        )

    def _read(
        self, ti: TaskInstance, try_number: int, metadata: LogMetadata | None = None
    ) -> tuple[OsLogMsgType, LogMetadata]:
        """
        Endpoint for streaming log.

        :param ti: task instance object
        :param try_number: try_number of the task instance
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        :return: a list of tuple with host and log documents, metadata.
        """
        if not metadata:
            # LogMetadata(TypedDict) is used as type annotation for log_reader; added ignore to suppress mypy error
            metadata = {"offset": 0}  # type: ignore[assignment]
        metadata = cast("LogMetadata", metadata)

        if "offset" not in metadata:
            metadata["offset"] = 0

        offset = metadata["offset"]
        log_id = self._render_log_id(ti, try_number)
        response = self._os_read(log_id, offset, ti)
        if response is not None and response.hits:
            logs_by_host = self._group_logs_by_host(response)
            next_offset = attrgetter(self.offset_field)(response[-1])
        else:
            logs_by_host = None
            next_offset = offset

        # Ensure a string here. Large offset numbers will get JSON.parsed incorrectly
        # on the client. Sending as a string prevents this issue.
        # https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/MAX_SAFE_INTEGER
        metadata["offset"] = str(next_offset)

        # end_of_log_mark may contain characters like '\n' which is needed to
        # have the log uploaded but will not be stored in elasticsearch.
        metadata["end_of_log"] = False
        if logs_by_host:
            if any(x[-1].message == self.end_of_log_mark for x in logs_by_host.values()):
                metadata["end_of_log"] = True

        cur_ts = pendulum.now()
        if "last_log_timestamp" in metadata:
            last_log_ts = timezone.parse(metadata["last_log_timestamp"])

            # if we are not getting any logs at all after more than N seconds of trying,
            # assume logs do not exist
            if int(next_offset) == 0 and cur_ts.diff(last_log_ts).in_seconds() > 5:
                metadata["end_of_log"] = True
                missing_log_message = (
                    f"*** Log {log_id} not found in Opensearch. "
                    "If your task started recently, please wait a moment and reload this page. "
                    "Otherwise, the logs for this task instance may have been removed."
                )
                if AIRFLOW_V_3_0_PLUS:
                    from airflow.utils.log.file_task_handler import StructuredLogMessage

                    # return list of StructuredLogMessage for Airflow 3.0+
                    return [StructuredLogMessage(event=missing_log_message)], metadata

                return [("", missing_log_message)], metadata  # type: ignore[list-item]
            if (
                # Assume end of log after not receiving new log for N min,
                cur_ts.diff(last_log_ts).in_minutes() >= 5
                # if max_offset specified, respect it
                or ("max_offset" in metadata and int(offset) >= int(metadata["max_offset"]))
            ):
                metadata["end_of_log"] = True

        if int(offset) != int(next_offset) or "last_log_timestamp" not in metadata:
            metadata["last_log_timestamp"] = str(cur_ts)

        # If we hit the end of the log, remove the actual end_of_log message
        # to prevent it from showing in the UI.
        def concat_logs(hits: list[Hit]):
            log_range = (len(hits) - 1) if hits[-1].message == self.end_of_log_mark else len(hits)
            return "\n".join(self._format_msg(hits[i]) for i in range(log_range))

        if logs_by_host:
            if AIRFLOW_V_3_0_PLUS:
                from airflow.utils.log.file_task_handler import StructuredLogMessage

                header = [
                    StructuredLogMessage(
                        event="::group::Log message source details",
                        sources=[host for host in logs_by_host.keys()],
                    ),  # type: ignore[call-arg]
                    StructuredLogMessage(event="::endgroup::"),
                ]

                # Flatten all hits, filter to only desired fields, and construct StructuredLogMessage objects
                message = header + [
                    StructuredLogMessage(
                        **{k: v for k, v in hit.to_dict().items() if k.lower() in TASK_LOG_FIELDS}
                    )
                    for hits in logs_by_host.values()
                    for hit in hits
                ]
            else:
                message = [(host, concat_logs(hits)) for host, hits in logs_by_host.items()]  # type: ignore[misc]
        else:
            message = []
        return message, metadata

    def _os_read(self, log_id: str, offset: int | str, ti: TaskInstance) -> OpensearchResponse | None:
        """
        Return the logs matching log_id in Elasticsearch and next offset or ''.

        :param log_id: the log_id of the log to read.
        :param offset: the offset start to read log from.
        :param ti: the task instance object

        :meta private:
        """
        query: dict[Any, Any] = {
            "query": {
                "bool": {
                    "filter": [{"range": {self.offset_field: {"gt": int(offset)}}}],
                    "must": [{"match_phrase": {"log_id": log_id}}],
                }
            }
        }
        index_patterns = self._get_index_patterns(ti)
        try:
            max_log_line = self.client.count(index=index_patterns, body=query)["count"]
        except NotFoundError as e:
            self.log.exception("The target index pattern %s does not exist", index_patterns)
            raise e

        if max_log_line != 0:
            try:
                res = self.client.search(
                    index=index_patterns,
                    body=query,
                    sort=[self.offset_field],
                    size=self.MAX_LINE_PER_PAGE,
                    from_=self.MAX_LINE_PER_PAGE * self.PAGE,
                )
                return OpensearchResponse(self, res)
            except Exception as err:
                self.log.exception("Could not read log with log_id: %s. Exception: %s", log_id, err)

        return None

    def _get_index_patterns(self, ti: TaskInstance | None) -> str:
        """
        Get index patterns by calling index_patterns_callable, if provided, or the configured index_patterns.

        :param ti: A TaskInstance object or None.
        """
        if self.index_patterns_callable:
            self.log.debug("Using index_patterns_callable: %s", self.index_patterns_callable)
            index_pattern_callable_obj = import_string(self.index_patterns_callable)
            return index_pattern_callable_obj(ti)
        self.log.debug("Using index_patterns: %s", self.index_patterns)
        return self.index_patterns

    def _get_result(self, hit: dict[Any, Any], parent_class=None) -> Hit:
        """
        Process a hit (i.e., a result) from an Elasticsearch response and transform it into a class instance.

        The transformation depends on the contents of the hit. If the document in hit contains a nested field,
        the '_resolve_nested' method is used to determine the appropriate class (based on the nested path).
        If the hit has a document type that is present in the '_doc_type_map', the corresponding class is
        used. If not, the method iterates over the '_doc_type' classes and uses the first one whose '_matches'
        method returns True for the hit.

        If the hit contains any 'inner_hits', these are also processed into 'ElasticSearchResponse' instances
        using the determined class.

        Finally, the transformed hit is returned. If the determined class has a 'from_es' method, this is
        used to transform the hit

        An example of the hit argument:

        {'_id': 'jdeZT4kBjAZqZnexVUxk',
         '_index': '.ds-filebeat-8.8.2-2023.07.09-000001',
         '_score': 2.482621,
         '_source': {'@timestamp': '2023-07-13T14:13:15.140Z',
                     'asctime': '2023-07-09T07:47:43.907+0000',
                     'container': {'id': 'airflow'},
                     'dag_id': 'example_bash_operator',
                     'ecs': {'version': '8.0.0'},
                     'logical_date': '2023_07_09T07_47_32_000000',
                     'filename': 'taskinstance.py',
                     'input': {'type': 'log'},
                     'levelname': 'INFO',
                     'lineno': 1144,
                     'log': {'file': {'path': "/opt/airflow/Documents/GitHub/airflow/logs/
                     dag_id=example_bash_operator'/run_id=owen_run_run/
                     task_id=run_after_loop/attempt=1.log"},
                             'offset': 0},
                     'log.offset': 1688888863907337472,
                     'log_id': 'example_bash_operator-run_after_loop-owen_run_run--1-1',
                     'message': 'Dependencies all met for dep_context=non-requeueable '
                                'deps ti=<TaskInstance: '
                                'example_bash_operator.run_after_loop owen_run_run '
                                '[queued]>',
                     'task_id': 'run_after_loop',
                     'try_number': '1'},
         '_type': '_doc'}
        """
        doc_class = Hit
        dt = hit.get("_type")

        if "_nested" in hit:
            doc_class = self._resolve_nested(hit, parent_class)

        elif dt in self._doc_type_map:
            doc_class = self._doc_type_map[dt]

        else:
            for doc_type in self._doc_type:
                if hasattr(doc_type, "_matches") and doc_type._matches(hit):
                    doc_class = doc_type
                    break

        for t in hit.get("inner_hits", ()):
            hit["inner_hits"][t] = OpensearchResponse(self, hit["inner_hits"][t], doc_class=doc_class)

        # callback should get the Hit class if "from_es" is not defined
        callback: type[Hit] | Callable[..., Any] = getattr(doc_class, "from_es", doc_class)
        return callback(hit)

    def _resolve_nested(self, hit: dict[Any, Any], parent_class=None) -> type[Hit]:
        """
        Resolve nested hits from Elasticsearch by iteratively navigating the `_nested` field.

        The result is used to fetch the appropriate document class to handle the hit.

        This method can be used with nested Elasticsearch fields which are structured
        as dictionaries with "field" and "_nested" keys.
        """
        doc_class = Hit

        nested_path: list[str] = []
        nesting = hit["_nested"]
        while nesting and "field" in nesting:
            nested_path.append(nesting["field"])
            nesting = nesting.get("_nested")
        nested_path_str = ".".join(nested_path)

        if hasattr(parent_class, "_index"):
            nested_field = parent_class._index.resolve_field(nested_path_str)

        if nested_field is not None:
            return nested_field._doc_class

        return doc_class

    def _group_logs_by_host(self, response: OpensearchResponse) -> dict[str, list[Hit]]:
        grouped_logs = defaultdict(list)
        for hit in response:
            key = getattr_nested(hit, self.host_field, None) or self.host
            grouped_logs[key].append(hit)
        return grouped_logs

    def _format_msg(self, hit: Hit):
        """Format ES Record to match settings.LOG_FORMAT when used with json_format."""
        # Using formatter._style.format makes it future proof i.e.
        # if we change the formatter style from '%' to '{' or '$', this will still work
        if self.json_format:
            with contextlib.suppress(Exception):
                return self.formatter._style.format(
                    logging.makeLogRecord({**LOG_LINE_DEFAULTS, **hit.to_dict()})
                )

        # Just a safe-guard to preserve backwards-compatibility
        return hit.message

    @property
    def supports_external_link(self) -> bool:
        """
        Whether we can support external links.

        TODO: It should support frontend just like ElasticSearchTaskhandler.
        """
        return False

    def get_external_log_url(self, task_instance, try_number) -> str:
        """
        Create an address for an external log collecting service.

        TODO: It should support frontend just like ElasticSearchTaskhandler.
        """
        return ""

    @property
    def log_name(self) -> str:
        """The log name."""
        return self.LOG_NAME

    @staticmethod
    def format_url(host: str) -> str:
        """
        Format the given host string to ensure it starts with 'http' and check if it represents a valid URL.

        :params host: The host string to format and check.
        """
        parsed_url = urlparse(host)

        if parsed_url.scheme not in ("http", "https"):
            host = "http://" + host
            parsed_url = urlparse(host)

        if not parsed_url.netloc:
            raise ValueError(f"'{host}' is not a valid URL.")

        return host
