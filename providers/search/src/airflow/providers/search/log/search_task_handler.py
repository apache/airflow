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
"""Task logs stored in Elasticsearch or OpenSearch."""

from __future__ import annotations

import contextlib
import json
import os
import sys
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import attrs

from airflow.providers.common.compat.module_loading import import_string
from airflow.providers.common.compat.sdk import conf
from airflow.providers.search.client import (
    BulkIndexError,
    NotFoundError,
    SearchClient,
    build_ssl_context,
    parse_cloud_id,
)
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import ExternalLoggingMixin, LoggingMixin

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI
    from airflow.utils.log.file_task_handler import LogMessages, LogSourceInfo

SECTION = "search"
DEFAULT_LOG_ID_TEMPLATE = "{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}"
TASK_LOG_FIELDS = ("timestamp", "event", "level", "chan", "logger", "error_detail", "message", "levelname")


def _render_log_id(log_id_template: str, ti: RuntimeTI | TaskInstance, try_number: int) -> str:
    return log_id_template.format(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=getattr(ti, "run_id", ""),
        try_number=try_number,
        map_index=getattr(ti, "map_index", ""),
    )


def _get_field(document: dict[str, Any], field: str) -> Any:
    """Return ``field`` from ``document``, following dots into nested objects when no flat key matches."""
    if field in document:
        return document[field]
    value: Any = document
    for part in field.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def _build_log_fields(document: dict[str, Any]) -> dict[str, Any]:
    """Reduce a stored log document to the fields of ``StructuredLogMessage``."""
    fields = {key: value for key, value in document.items() if key.lower() in TASK_LOG_FIELDS}
    if "@timestamp" in document and "timestamp" not in fields:
        fields["timestamp"] = document["@timestamp"]
    if "levelname" in fields and "level" not in fields:
        fields["level"] = fields.pop("levelname")
    if "event" not in fields:
        fields["event"] = fields.pop("message", "")
    if "error_detail" in fields and not fields["error_detail"]:
        fields.pop("error_detail")
    return fields


def create_client_from_config() -> SearchClient:
    """Build the client for task logs from the ``[search]`` configuration section."""
    host = conf.get(SECTION, "host", fallback="")
    cloud_id = conf.get(SECTION, "cloud_id", fallback="")
    if cloud_id:
        host = parse_cloud_id(cloud_id)
    if not host:
        raise ValueError("Set [search] host or [search] cloud_id to store task logs in a search engine.")
    username = conf.get(SECTION, "username", fallback="")
    api_key = conf.get(SECTION, "api_key", fallback="")
    return SearchClient(
        [item for item in host.split(",") if item.strip()],
        basic_auth=(username, conf.get(SECTION, "password", fallback="")) if username else None,
        api_key=api_key or None,
        ssl_context=build_ssl_context(
            verify_certs=conf.getboolean(SECTION, "verify_certs", fallback=True),
            ca_certs=conf.get(SECTION, "ca_certs", fallback="") or None,
            client_cert=conf.get(SECTION, "client_cert", fallback="") or None,
            client_key=conf.get(SECTION, "client_key", fallback="") or None,
        ),
        request_timeout=conf.getfloat(SECTION, "request_timeout", fallback=10.0),
        max_retries=conf.getint(SECTION, "max_retries", fallback=3),
        http_compress=conf.getboolean(SECTION, "http_compress", fallback=False),
    )


@attrs.define(kw_only=True)
class SearchRemoteLogIO(LoggingMixin):
    """
    Upload task logs to, and read them from, Elasticsearch or OpenSearch.

    Each log line is stored as one document carrying ``log_id`` (rendered from
    ``log_id_template``) and ``offset_field``, its line number within the upload. Reads query by
    ``log_id`` and page through the documents in ``offset_field`` order.

    :param base_log_folder: Local folder that task logs are written to.
    :param client: Client for the cluster.
    :param write_to_search: Index task logs into ``target_index`` after the task finishes.
    :param write_stdout: Also print task logs to stdout as JSON lines, for a log shipper to collect.
    :param delete_local_copy: Delete the local log after it is indexed.
    :param target_index: Index that task logs are written to.
    :param index_patterns: Indices that task logs are read from.
    :param index_patterns_callable: Dotted path to a callable that takes the task instance and
        returns the indices to read from; takes precedence over ``index_patterns``.
    :param log_id_template: Template rendered into the ``log_id`` of each document.
    :param host_field: Document field naming the worker that produced the line.
    :param offset_field: Document field holding the line number.
    :param page_size: Documents fetched per search request when reading.
    """

    base_log_folder: Path = attrs.field(converter=Path)
    client: SearchClient
    write_to_search: bool = False
    write_stdout: bool = False
    delete_local_copy: bool = False
    target_index: str = "airflow-logs"
    index_patterns: str = "_all"
    index_patterns_callable: str = ""
    log_id_template: str = DEFAULT_LOG_ID_TEMPLATE
    host_field: str = "host"
    offset_field: str = "offset"
    page_size: int = 1000

    processors = ()

    @classmethod
    def from_config(cls) -> SearchRemoteLogIO:
        """Build the remote log IO from the ``[logging]`` and ``[search]`` configuration sections."""
        return cls(
            base_log_folder=os.path.expanduser(conf.get_mandatory_value("logging", "base_log_folder")),
            delete_local_copy=conf.getboolean("logging", "delete_local_logs"),
            client=create_client_from_config(),
            write_to_search=conf.getboolean(SECTION, "write_to_search", fallback=False),
            write_stdout=conf.getboolean(SECTION, "write_stdout", fallback=False),
            target_index=conf.get(SECTION, "target_index", fallback="airflow-logs"),
            index_patterns=conf.get(SECTION, "index_patterns", fallback="_all"),
            index_patterns_callable=conf.get(SECTION, "index_patterns_callable", fallback=""),
            log_id_template=conf.get(SECTION, "log_id_template", fallback="") or DEFAULT_LOG_ID_TEMPLATE,
            host_field=conf.get(SECTION, "host_field", fallback="host"),
            offset_field=conf.get(SECTION, "offset_field", fallback="offset"),
            page_size=conf.getint(SECTION, "max_lines_per_page", fallback=1000),
        )

    def upload(self, path: os.PathLike | str, ti: RuntimeTI | None = None) -> None:
        """Print and/or index the task log at ``path``."""
        if ti is None or not (self.write_stdout or self.write_to_search):
            return
        local_loc = Path(path)
        if not local_loc.is_absolute():
            local_loc = self.base_log_folder / local_loc
        if not local_loc.is_file():
            return

        log_id = _render_log_id(self.log_id_template, ti, ti.try_number)
        documents = self._parse_raw_log(local_loc.read_text(), log_id)
        if self.write_stdout:
            for document in documents:
                sys.stdout.write(json.dumps(document) + "\n")
            sys.stdout.flush()
        if self.write_to_search and self._index_documents(documents) and self.delete_local_copy:
            self._delete_local_log(local_loc)

    def _parse_raw_log(self, log: str, log_id: str) -> list[dict[str, Any]]:
        documents = []
        for offset, line in enumerate((line for line in log.splitlines() if line.strip()), start=1):
            try:
                document = json.loads(line)
            except json.JSONDecodeError:
                document = None
            if not isinstance(document, dict):
                document = {"event": line}
            document.update({"log_id": log_id, self.offset_field: offset})
            documents.append(document)
        return documents

    def _index_documents(self, documents: list[dict[str, Any]]) -> bool:
        """Index ``documents``; return whether every one was stored."""
        try:
            self.client.bulk({"_index": self.target_index, "_source": doc} for doc in documents)
        except BulkIndexError as err:
            self.log.error("Failed to index %d of %d task log lines", len(err.errors), len(documents))
            for error in err.errors[:10]:
                self.log.error("Indexing error: %s", error)
            return False
        except Exception:
            self.log.exception("Unable to index task logs into %s", self.target_index)
            return False
        return True

    def _delete_local_log(self, local_loc: Path) -> None:
        """Delete the uploaded file and any directories it leaves empty below ``base_log_folder``."""
        local_loc.unlink(missing_ok=True)
        parent = local_loc.parent
        while parent != self.base_log_folder and self.base_log_folder in parent.parents:
            with contextlib.suppress(OSError):
                parent.rmdir()
            if parent.exists():
                break
            parent = parent.parent

    def read(self, relative_path: str, ti: RuntimeTI) -> tuple[LogSourceInfo, LogMessages]:
        sources, streams = self.stream(relative_path, ti)
        return sources, [line for stream in streams for line in stream]

    def stream(
        self, relative_path: str, ti: RuntimeTI
    ) -> tuple[LogSourceInfo, list[Generator[str, None, None]]]:
        log_id = _render_log_id(self.log_id_template, ti, ti.try_number)
        index = self._get_index_patterns(ti)
        first_page = self._search_page(index, log_id, after_offset=0)
        if not first_page:
            return [f"Log {log_id} not found in {index}"], []
        hosts = {_get_field(document, self.host_field) for document in first_page}
        sources = [f"{log_id} from {', '.join(sorted(str(h) for h in hosts if h)) or index}"]
        return sources, [self._iter_log_lines(index, log_id, first_page)]

    def _iter_log_lines(
        self, index: str, log_id: str, page: list[dict[str, Any]]
    ) -> Generator[str, None, None]:
        while page:
            for document in page:
                yield json.dumps(_build_log_fields(document))
            if len(page) < self.page_size:
                return
            page = self._search_page(index, log_id, after_offset=page[-1][self.offset_field])

    def _search_page(self, index: str, log_id: str, *, after_offset: int) -> list[dict[str, Any]]:
        body = {
            "query": {
                "bool": {
                    "filter": [{"range": {self.offset_field: {"gt": after_offset}}}],
                    "must": [{"match_phrase": {"log_id": log_id}}],
                }
            },
            "sort": [{self.offset_field: "asc"}],
            "size": self.page_size,
            "_source": list(
                dict.fromkeys(["@timestamp", *TASK_LOG_FIELDS, self.host_field, self.offset_field])
            ),
        }
        try:
            response = self.client.search(index, body)
        except NotFoundError:
            self.log.warning("Index pattern %s does not exist", index)
            return []
        return [hit["_source"] for hit in response["hits"]["hits"]]

    def _get_index_patterns(self, ti: RuntimeTI | None) -> str:
        if self.index_patterns_callable:
            return import_string(self.index_patterns_callable)(ti)
        return self.index_patterns


class SearchTaskHandler(FileTaskHandler, ExternalLoggingMixin):
    """
    Task log handler that links to a search UI such as Kibana or OpenSearch Dashboards.

    Reading and writing go through :class:`SearchRemoteLogIO`. This handler only adds the external
    link, so it is needed only when ``frontend`` is set.

    :param base_log_folder: Local folder that task logs are written to.
    :param frontend: URL template of the log view, with ``{log_id}`` as placeholder, e.g.
        ``https://kibana.example.com/app/discover#/?_a=(query:(query:'log_id:"{log_id}"'))``.
    :param log_id_template: Template rendered into ``{log_id}``.
    """

    LOG_NAME = "Search"

    def __init__(
        self,
        base_log_folder: str,
        frontend: str = "",
        log_id_template: str = DEFAULT_LOG_ID_TEMPLATE,
        **kwargs: Any,
    ) -> None:
        super().__init__(base_log_folder=base_log_folder, **kwargs)
        self.frontend = frontend
        self.log_id_template = log_id_template

    @property
    def log_name(self) -> str:
        return self.LOG_NAME

    @property
    def supports_external_link(self) -> bool:
        return bool(self.frontend)

    def get_external_log_url(self, task_instance: TaskInstance, try_number: int) -> str:
        log_id = _render_log_id(self.log_id_template, task_instance, try_number)
        scheme = "" if "://" in self.frontend else "https://"
        return scheme + self.frontend.format(log_id=quote(log_id))
