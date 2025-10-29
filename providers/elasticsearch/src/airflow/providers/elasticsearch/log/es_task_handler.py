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

import contextlib
import inspect
import json
import logging
import os
import shutil
import sys
import time
from collections import defaultdict
from collections.abc import Callable
from operator import attrgetter
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast
from urllib.parse import quote, urlparse

import attrs

# Using `from elasticsearch import *` would break elasticsearch mocking used in unit test.
import elasticsearch
import pendulum
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError

import airflow.logging_config as alc
from airflow.configuration import conf
from airflow.models.dagrun import DagRun
from airflow.providers.elasticsearch.log.es_json_formatter import ElasticsearchJSONFormatter
from airflow.providers.elasticsearch.log.es_response import ElasticSearchResponse, Hit, resolve_nested
from airflow.providers.elasticsearch.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils import timezone
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import ExternalLoggingMixin, LoggingMixin
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI
    from airflow.utils.log.file_task_handler import LogMessages, LogMetadata, LogSourceInfo


if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.log.file_task_handler import StructuredLogMessage

    EsLogMsgType = list[StructuredLogMessage] | str
else:
    EsLogMsgType = list[tuple[str, str]]  # type: ignore[assignment,misc]


LOG_LINE_DEFAULTS = {"exc_text": "", "stack_info": ""}
# Elasticsearch hosted log type

# Compatibility: Airflow 2.3.3 and up uses this method, which accesses the
# LogTemplate model to record the log ID template used. If this function does
# not exist, the task handler should use the log_id_template attribute instead.
USE_PER_RUN_LOG_ID = hasattr(DagRun, "get_log_template")

TASK_LOG_FIELDS = ["timestamp", "event", "level", "chan", "logger"]

VALID_ES_CONFIG_KEYS = set(inspect.signature(elasticsearch.Elasticsearch.__init__).parameters.keys())
# Remove `self` from the valid set of kwargs
VALID_ES_CONFIG_KEYS.remove("self")


def get_es_kwargs_from_config() -> dict[str, Any]:
    elastic_search_config = conf.getsection("elasticsearch_configs")
    kwargs_dict = (
        {key: value for key, value in elastic_search_config.items() if key in VALID_ES_CONFIG_KEYS}
        if elastic_search_config
        else {}
    )
    return kwargs_dict


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


def _render_log_id(log_id_template: str, ti: TaskInstance | TaskInstanceKey, try_number: int) -> str:
    return log_id_template.format(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=getattr(ti, "run_id", ""),
        try_number=try_number,
        map_index=getattr(ti, "map_index", ""),
    )


def _clean_date(value: datetime | None) -> str:
    """
    Clean up a date value so that it is safe to query in elasticsearch by removing reserved characters.

    https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
    """
    if value is None:
        return ""
    return value.strftime("%Y_%m_%dT%H_%M_%S_%f")


class ElasticsearchTaskHandler(FileTaskHandler, ExternalLoggingMixin, LoggingMixin):
    """
    ElasticsearchTaskHandler is a python log handler that reads logs from Elasticsearch.

    Note that Airflow by default does not handle the indexing of logs into Elasticsearch. Instead,
    Airflow flushes logs into local files. Additional software setup is required to index
    the logs into Elasticsearch, such as using Filebeat and Logstash.

    Airflow can be configured to support directly writing logging to Elasticsearch. To enable this feature,
    set `json_format` and `write_to_es` to `True`.

    To efficiently query and sort Elasticsearch results, this handler assumes each
    log message has a field `log_id` consists of ti primary keys:
    `log_id = {dag_id}-{task_id}-{logical_date}-{try_number}`
    Log messages with specific log_id are sorted based on `offset`,
    which is a unique integer indicates log message's order.
    Timestamps here are unreliable because multiple log messages
    might have the same timestamp.

    :param base_log_folder: base folder to store logs locally
    :param log_id_template: log id template
    :param host: Elasticsearch host name
    """

    PAGE = 0
    MAX_LINE_PER_PAGE = 1000
    LOG_NAME = "Elasticsearch"

    trigger_should_wrap = True

    def __init__(
        self,
        base_log_folder: str,
        end_of_log_mark: str,
        write_stdout: bool,
        json_fields: str,
        json_format: bool = False,
        write_to_es: bool = False,
        target_index: str = "airflow-logs",
        host_field: str = "host",
        offset_field: str = "offset",
        host: str = "http://localhost:9200",
        frontend: str = "localhost:5601",
        index_patterns: str = conf.get("elasticsearch", "index_patterns"),
        index_patterns_callable: str = conf.get("elasticsearch", "index_patterns_callable", fallback=""),
        es_kwargs: dict | None | Literal["default_es_kwargs"] = "default_es_kwargs",
        max_bytes: int = 0,
        backup_count: int = 0,
        delay: bool = False,
        **kwargs,
    ) -> None:
        es_kwargs = es_kwargs or {}
        if es_kwargs == "default_es_kwargs":
            es_kwargs = get_es_kwargs_from_config()
        self.host = self.format_url(host)
        # support log file size handling of FileTaskHandler
        super().__init__(
            base_log_folder=base_log_folder, max_bytes=max_bytes, backup_count=backup_count, delay=delay
        )
        self.closed = False

        self.client = elasticsearch.Elasticsearch(self.host, **es_kwargs)
        # in airflow.cfg, host of elasticsearch has to be http://dockerhostXxxx:9200

        self.frontend = frontend
        self.mark_end_on_close = True
        self.end_of_log_mark = end_of_log_mark.strip()
        self.write_stdout = write_stdout
        self.json_format = json_format
        self.json_fields = [label.strip() for label in json_fields.split(",")]
        self.host_field = host_field
        self.offset_field = offset_field
        self.index_patterns = index_patterns
        self.index_patterns_callable = index_patterns_callable
        self.context_set = False
        self.write_to_es = write_to_es
        self.target_index = target_index
        self.delete_local_copy = kwargs.get(
            "delete_local_copy", conf.getboolean("logging", "delete_local_logs")
        )

        self.formatter: logging.Formatter
        self.handler: logging.FileHandler | logging.StreamHandler | None = None
        self._doc_type_map: dict[Any, Any] = {}
        self._doc_type: list[Any] = []
        self.log_id_template: str = conf.get(
            "elasticsearch",
            "log_id_template",
            fallback="{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}",
        )
        self.io = ElasticsearchRemoteLogIO(
            host=self.host,
            target_index=self.target_index,
            write_stdout=self.write_stdout,
            write_to_es=self.write_to_es,
            offset_field=self.offset_field,
            host_field=self.host_field,
            base_log_folder=base_log_folder,
            delete_local_copy=self.delete_local_copy,
            log_id_template=self.log_id_template,
        )
        # Airflow 3 introduce REMOTE_TASK_LOG for handling remote logging
        # REMOTE_TASK_LOG should be explicitly set in airflow_local_settings.py when trying to use ESTaskHandler
        # Before airflow 3.1, REMOTE_TASK_LOG is not set when trying to use ES TaskHandler.
        if AIRFLOW_V_3_0_PLUS and alc.REMOTE_TASK_LOG is None:
            alc.REMOTE_TASK_LOG = self.io

    @staticmethod
    def format_url(host: str) -> str:
        """
        Format the given host string to ensure it starts with 'http' and check if it represents a valid URL.

        :params host: The host string to format and check.
        """
        parsed_url = urlparse(host)

        # Check if the scheme is either http or https
        # Handles also the Python 3.9+ case where urlparse understands "localhost:9200"
        # differently than urlparse in Python 3.8 and below (https://github.com/psf/requests/issues/6455)
        if parsed_url.scheme not in ("http", "https"):
            host = "http://" + host
            parsed_url = urlparse(host)

        # Basic validation for a valid URL
        if not parsed_url.netloc:
            raise ValueError(f"'{host}' is not a valid URL.")

        return host

    def _read_grouped_logs(self):
        return True

    def _read(
        self, ti: TaskInstance, try_number: int, metadata: LogMetadata | None = None
    ) -> tuple[EsLogMsgType, LogMetadata]:
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
        log_id = _render_log_id(self.log_id_template, ti, try_number)
        response = self.io._es_read(log_id, offset, ti)
        # TODO: Can we skip group logs by host ?
        if response is not None and response.hits:
            logs_by_host = self.io._group_logs_by_host(response)
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
            end_mark_found = any(
                self._get_log_message(x[-1]) == self.end_of_log_mark for x in logs_by_host.values()
            )
            if end_mark_found:
                metadata["end_of_log"] = True

        cur_ts = pendulum.now()
        if "last_log_timestamp" in metadata:
            last_log_ts = timezone.parse(metadata["last_log_timestamp"])

            # if we are not getting any logs at all after more than N seconds of trying,
            # assume logs do not exist
            if int(next_offset) == 0 and cur_ts.diff(last_log_ts).in_seconds() > 5:
                metadata["end_of_log"] = True
                missing_log_message = (
                    f"*** Log {log_id} not found in Elasticsearch. "
                    "If your task started recently, please wait a moment and reload this page. "
                    "Otherwise, the logs for this task instance may have been removed."
                )
                if AIRFLOW_V_3_0_PLUS:
                    from airflow.utils.log.file_task_handler import StructuredLogMessage

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
                message = [
                    (host, self.concat_logs(hits))  # type: ignore[misc]
                    for host, hits in logs_by_host.items()
                ]
        else:
            message = []
            metadata["end_of_log"] = True
        return message, metadata

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
        return self._get_log_message(hit)

    def emit(self, record):
        if self.handler:
            setattr(record, self.offset_field, int(time.time() * (10**9)))
            self.handler.emit(record)

    def set_context(self, ti: TaskInstance, *, identifier: str | None = None) -> None:
        """
        TODO: This API should be removed in airflow 3.

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
            self.formatter = ElasticsearchJSONFormatter(
                fmt=self.formatter._fmt,
                json_fields=[*self.json_fields, self.offset_field],
                extras={
                    "dag_id": str(ti.dag_id),
                    "task_id": str(ti.task_id),
                    date_key: (
                        _clean_date(ti.logical_date) if AIRFLOW_V_3_0_PLUS else _clean_date(ti.execution_date)
                    ),
                    "try_number": str(ti.try_number),
                    "log_id": _render_log_id(self.log_id_template, ti, ti.try_number),
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

    def close(self) -> None:
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        # TODO: This API should be simplified since Airflow 3 no longer requires this API for writing log to ES
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

        if self.io.write_stdout:
            self.handler.close()
            sys.stdout = sys.__stdout__

        super().close()

        self.closed = True

    @property
    def log_name(self) -> str:
        """The log name."""
        return self.LOG_NAME

    def get_external_log_url(self, task_instance: TaskInstance, try_number: int) -> str:
        """
        Create an address for an external log collecting service.

        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from.
        :return: URL to the external log collection service
        """
        log_id = _render_log_id(self.log_id_template, task_instance, try_number)
        scheme = "" if "://" in self.frontend else "https://"
        return scheme + self.frontend.format(log_id=quote(log_id))

    @property
    def supports_external_link(self) -> bool:
        """Whether we can support external links."""
        return bool(self.frontend)

    def _get_result(self, hit: dict[Any, Any], parent_class=None) -> Hit:
        """
        Process a hit (i.e., a result) from an Elasticsearch response and transform it into a class instance.

        The transformation depends on the contents of the hit. If the document in hit contains a nested field,
        the 'resolve_nested' method is used to determine the appropriate class (based on the nested path).
        If the hit has a document type that is present in the '_doc_type_map', the corresponding class is
        used. If not, the method iterates over the '_doc_type' classes and uses the first one whose '_matches'
        method returns True for the hit.

        If the hit contains any 'inner_hits', these are also processed into 'ElasticSearchResponse' instances
        using the determined class.

        Finally, the transformed hit is returned. If the determined class has a 'from_es' method, this is
        used to transform the hit
        """
        doc_class = Hit
        dt = hit.get("_type")

        if "_nested" in hit:
            doc_class = resolve_nested(hit, parent_class)

        elif dt in self._doc_type_map:
            doc_class = self._doc_type_map[dt]

        else:
            for doc_type in self._doc_type:
                if hasattr(doc_type, "_matches") and doc_type._matches(hit):
                    doc_class = doc_type
                    break

        for t in hit.get("inner_hits", ()):
            hit["inner_hits"][t] = ElasticSearchResponse(self, hit["inner_hits"][t], doc_class=doc_class)

        # callback should get the Hit class if "from_es" is not defined
        callback: type[Hit] | Callable[..., Any] = getattr(doc_class, "from_es", doc_class)
        return callback(hit)

    def _get_log_message(self, hit: Hit) -> str:
        """
        Get log message from hit, supporting both Airflow 2.x and 3.x formats.

        In Airflow 2.x, the log record JSON has a "message" key, e.g.:
        {
          "message": "Dag name:dataset_consumes_1 queued_at:2025-08-12 15:05:57.703493+00:00",
          "offset": 1755011166339518208,
          "log_id": "dataset_consumes_1-consuming_1-manual__2025-08-12T15:05:57.691303+00:00--1-1"
        }

        In Airflow 3.x, the "message" field is renamed to "event".
        We check the correct attribute depending on the Airflow major version.
        """
        if hasattr(hit, "event"):
            return hit.event
        if hasattr(hit, "message"):
            return hit.message
        return ""

    def concat_logs(self, hits: list[Hit]) -> str:
        log_range = (len(hits) - 1) if self._get_log_message(hits[-1]) == self.end_of_log_mark else len(hits)
        return "\n".join(self._format_msg(hits[i]) for i in range(log_range))


@attrs.define(kw_only=True)
class ElasticsearchRemoteLogIO(LoggingMixin):  # noqa: D101
    json_format: bool = False
    write_stdout: bool = False
    delete_local_copy: bool = False
    host: str = "http://localhost:9200"
    host_field: str = "host"
    target_index: str = "airflow-logs"
    offset_field: str = "offset"
    write_to_es: bool = False
    base_log_folder: Path = attrs.field(converter=Path)
    log_id_template: str = conf.get(
        "elasticsearch",
        "log_id_template",
        fallback="{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}",
    )

    processors = ()

    def __attrs_post_init__(self):
        es_kwargs = get_es_kwargs_from_config()
        self.client = elasticsearch.Elasticsearch(self.host, **es_kwargs)
        self.index_patterns_callable = conf.get("elasticsearch", "index_patterns_callable", fallback="")
        self.PAGE = 0
        self.MAX_LINE_PER_PAGE = 1000
        self.index_patterns: str = conf.get("elasticsearch", "index_patterns")
        self._doc_type_map: dict[Any, Any] = {}
        self._doc_type: list[Any] = []

    def upload(self, path: os.PathLike | str, ti: RuntimeTI):
        """Write the log to ElasticSearch."""
        path = Path(path)

        if path.is_absolute():
            local_loc = path
        else:
            local_loc = self.base_log_folder.joinpath(path)

        log_id = _render_log_id(self.log_id_template, ti, ti.try_number)  # type: ignore[arg-type]
        if local_loc.is_file() and self.write_stdout:
            # Intentionally construct the log_id and offset field

            log_lines = self._parse_raw_log(local_loc.read_text(), log_id)
            for line in log_lines:
                sys.stdout.write(json.dumps(line) + "\n")
                sys.stdout.flush()

        if local_loc.is_file() and self.write_to_es:
            log_lines = self._parse_raw_log(local_loc.read_text(), log_id)
            success = self._write_to_es(log_lines)
            if success and self.delete_local_copy:
                shutil.rmtree(os.path.dirname(local_loc))

    def _parse_raw_log(self, log: str, log_id: str) -> list[dict[str, Any]]:
        logs = log.split("\n")
        parsed_logs = []
        offset = 1
        for line in logs:
            # Make sure line is not empty
            if line.strip():
                # construct log_id which is {dag_id}-{task_id}-{run_id}-{map_index}-{try_number}
                # also construct the offset field (default is 'offset')
                log_dict = json.loads(line)
                log_dict.update({"log_id": log_id, self.offset_field: offset})
                offset += 1
                parsed_logs.append(log_dict)

        return parsed_logs

    def _write_to_es(self, log_lines: list[dict[str, Any]]) -> bool:
        """
        Write the log to ElasticSearch; return `True` or fails silently and return `False`.

        :param log_lines: the log_lines to write to the ElasticSearch.
        """
        # Prepare the bulk request for Elasticsearch
        bulk_actions = [{"_index": self.target_index, "_source": log} for log in log_lines]
        try:
            _ = helpers.bulk(self.client, bulk_actions)
            return True
        except helpers.BulkIndexError as bie:
            self.log.exception("Bulk upload failed for %d log(s)", len(bie.errors))
            for error in bie.errors:
                self.log.exception(error)
            return False
        except Exception as e:
            self.log.exception("Unable to insert logs into Elasticsearch. Reason: %s", str(e))
            return False

    def read(self, _relative_path: str, ti: RuntimeTI) -> tuple[LogSourceInfo, LogMessages]:
        log_id = _render_log_id(self.log_id_template, ti, ti.try_number)  # type: ignore[arg-type]
        self.log.info("Reading log %s from Elasticsearch", log_id)
        offset = 0
        response = self._es_read(log_id, offset, ti)
        if response is not None and response.hits:
            logs_by_host = self._group_logs_by_host(response)
        else:
            logs_by_host = None

        if logs_by_host is None:
            missing_log_message = (
                f"*** Log {log_id} not found in Elasticsearch. "
                "If your task started recently, please wait a moment and reload this page. "
                "Otherwise, the logs for this task instance may have been removed."
            )
            return [], [missing_log_message]

        header = []
        # Start log group
        header.append("".join([host for host in logs_by_host.keys()]))

        message = []
        # Structured log messages
        for hits in logs_by_host.values():
            for hit in hits:
                filtered = {k: v for k, v in hit.to_dict().items() if k.lower() in TASK_LOG_FIELDS}
                message.append(json.dumps(filtered))

        return header, message

    def _es_read(self, log_id: str, offset: int | str, ti: RuntimeTI) -> ElasticSearchResponse | None:
        """
        Return the logs matching log_id in Elasticsearch and next offset or ''.

        :param log_id: the log_id of the log to read.
        :param offset: the offset start to read log from.
        :param ti: the task instance object

        :meta private:
        """
        query: dict[Any, Any] = {
            "bool": {
                "filter": [{"range": {self.offset_field: {"gt": int(offset)}}}],
                "must": [{"match_phrase": {"log_id": log_id}}],
            }
        }

        index_patterns = self._get_index_patterns(ti)
        try:
            max_log_line = self.client.count(index=index_patterns, query=query)["count"]
        except NotFoundError as e:
            self.log.exception("The target index pattern %s does not exist", index_patterns)
            raise e

        if max_log_line != 0:
            try:
                res = self.client.search(
                    index=index_patterns,
                    query=query,
                    sort=[self.offset_field],
                    size=self.MAX_LINE_PER_PAGE,
                    from_=self.MAX_LINE_PER_PAGE * self.PAGE,
                )
                return ElasticSearchResponse(self, res)
            except Exception as err:
                self.log.exception("Could not read log with log_id: %s. Exception: %s", log_id, err)

        return None

    def _get_index_patterns(self, ti: RuntimeTI | None) -> str:
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

    def _group_logs_by_host(self, response: ElasticSearchResponse) -> dict[str, list[Hit]]:
        grouped_logs = defaultdict(list)
        for hit in response:
            key = getattr_nested(hit, self.host_field, None) or self.host
            grouped_logs[key].append(hit)
        return grouped_logs

    def _get_result(self, hit: dict[Any, Any], parent_class=None) -> Hit:
        """
        Process a hit (i.e., a result) from an Elasticsearch response and transform it into a class instance.

        The transformation depends on the contents of the hit. If the document in hit contains a nested field,
        the 'resolve_nested' method is used to determine the appropriate class (based on the nested path).
        If the hit has a document type that is present in the '_doc_type_map', the corresponding class is
        used. If not, the method iterates over the '_doc_type' classes and uses the first one whose '_matches'
        method returns True for the hit.

        If the hit contains any 'inner_hits', these are also processed into 'ElasticSearchResponse' instances
        using the determined class.

        Finally, the transformed hit is returned. If the determined class has a 'from_es' method, this is
        used to transform the hit
        """
        doc_class = Hit
        dt = hit.get("_type")

        if "_nested" in hit:
            doc_class = resolve_nested(hit, parent_class)

        elif dt in self._doc_type_map:
            doc_class = self._doc_type_map[dt]

        else:
            for doc_type in self._doc_type:
                if hasattr(doc_type, "_matches") and doc_type._matches(hit):
                    doc_class = doc_type
                    break

        for t in hit.get("inner_hits", ()):
            hit["inner_hits"][t] = ElasticSearchResponse(self, hit["inner_hits"][t], doc_class=doc_class)

        # callback should get the Hit class if "from_es" is not defined
        callback: type[Hit] | Callable[..., Any] = getattr(doc_class, "from_es", doc_class)
        return callback(hit)
