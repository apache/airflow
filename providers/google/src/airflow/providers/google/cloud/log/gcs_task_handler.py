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

import logging
import os
import shutil
from collections.abc import Collection
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING

import attrs

# Make mypy happy by importing as aliases
import google.cloud.storage as storage

from airflow.configuration import conf
from airflow.providers.common.compat.sdk import AirflowNotFoundException
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.providers.google.cloud.utils.credentials_provider import (
    get_credentials_and_project_id,
)
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.providers.google.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from io import TextIOWrapper

    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI
    from airflow.utils.log.file_task_handler import LogResponse, RawLogStream, StreamingLogResponse

_DEFAULT_SCOPESS = frozenset(
    [
        "https://www.googleapis.com/auth/devstorage.read_write",
    ]
)

logger = logging.getLogger(__name__)


@attrs.define
class GCSRemoteLogIO(LoggingMixin):  # noqa: D101
    remote_base: str
    base_log_folder: Path = attrs.field(converter=Path)
    delete_local_copy: bool
    project_id: str | None = None

    gcp_key_path: str | None = None
    gcp_keyfile_dict: dict | None = None
    scopes: Collection[str] | None = _DEFAULT_SCOPESS

    processors = ()

    def upload(self, path: os.PathLike | str, ti: RuntimeTI):
        """Upload the given log path to the remote storage."""
        path = Path(path)
        if path.is_absolute():
            local_loc = path
            remote_loc = os.path.join(self.remote_base, path.relative_to(self.base_log_folder))
        else:
            local_loc = self.base_log_folder.joinpath(path)
            remote_loc = os.path.join(self.remote_base, path)

        if local_loc.is_file():
            # read log and remove old logs to get just the latest additions
            log = local_loc.read_text()
            has_uploaded = self.write(log, remote_loc)
            if has_uploaded and self.delete_local_copy:
                shutil.rmtree(os.path.dirname(local_loc))

    @cached_property
    def hook(self) -> GCSHook | None:
        """Returns GCSHook if remote_log_conn_id configured."""
        conn_id = conf.get("logging", "remote_log_conn_id", fallback=None)
        if conn_id:
            try:
                return GCSHook(gcp_conn_id=conn_id)
            except AirflowNotFoundException:
                pass
        return None

    @cached_property
    def client(self) -> storage.Client:
        """Returns GCS Client."""
        if self.hook:
            credentials, project_id = self.hook.get_credentials_and_project_id()
        else:
            credentials, project_id = get_credentials_and_project_id(
                key_path=self.gcp_key_path,
                keyfile_dict=self.gcp_keyfile_dict,
                scopes=self.scopes,
                disable_logging=True,
            )
        return storage.Client(
            credentials=credentials,
            client_info=CLIENT_INFO,
            project=self.project_id if self.project_id else project_id,
        )

    def write(self, log: str, remote_log_location: str) -> bool:
        """
        Write the log to the remote location and return `True`; fail silently and return `False` on error.

        :param log: the log to write to the remote_log_location
        :param remote_log_location: the log's location in remote storage
        :return: whether the log is successfully written to remote location or not.
        """
        try:
            blob = storage.Blob.from_string(remote_log_location, self.client)
            old_log = blob.download_as_bytes().decode()
            log = f"{old_log}\n{log}" if old_log else log
        except Exception as e:
            if not self.no_log_found(e):
                self.log.warning("Error checking for previous log: %s", e)
        try:
            blob = storage.Blob.from_string(remote_log_location, self.client)
            blob.upload_from_string(log, content_type="text/plain")
        except Exception as e:
            self.log.error("Could not write logs to %s: %s", remote_log_location, e)
            return False
        return True

    @staticmethod
    def no_log_found(exc):
        """
        Given exception, determine whether it is result of log not found.

        :meta private:
        """
        return (exc.args and isinstance(exc.args[0], str) and "No such object" in exc.args[0]) or getattr(
            exc, "resp", {}
        ).get("status") == "404"

    def read(self, relative_path: str, ti: RuntimeTI) -> LogResponse:
        messages, log_streams = self.stream(relative_path, ti)
        if not log_streams:
            return messages, None

        logs: list[str] = []
        try:
            # for each log_stream, exhaust the generator into a string
            logs = ["".join(line for line in log_stream) for log_stream in log_streams]
        except Exception as e:
            if not AIRFLOW_V_3_0_PLUS:
                messages.append(f"Unable to read remote log {e}")

        return messages, logs

    def stream(self, relative_path: str, ti: RuntimeTI) -> StreamingLogResponse:
        messages: list[str] = []
        log_streams: list[RawLogStream] = []
        remote_loc = os.path.join(self.remote_base, relative_path)
        uris: list[str] = []
        bucket, prefix = _parse_gcs_url(remote_loc)
        blobs = list(self.client.list_blobs(bucket_or_name=bucket, prefix=prefix))

        if blobs:
            uris = [f"gs://{bucket}/{b.name}" for b in blobs]
            if AIRFLOW_V_3_0_PLUS:
                messages = uris
            else:
                messages.extend(["Found remote logs:", *[f"  * {x}" for x in sorted(uris)]])
        else:
            return messages, []

        try:
            for key in sorted(uris):
                blob = storage.Blob.from_string(key, self.client)
                stream = blob.open("r")
                log_streams.append(self._get_log_stream(stream))
        except Exception as e:
            if not AIRFLOW_V_3_0_PLUS:
                messages.append(f"Unable to read remote log {e}")
        return messages, log_streams

    def _get_log_stream(self, stream: TextIOWrapper) -> RawLogStream:
        """
        Yield lines from the given stream.

        :param stream: The opened stream to read from.
        :yield: Lines of the log file.
        """
        try:
            yield from stream
        finally:
            stream.close()


class GCSTaskHandler(FileTaskHandler, LoggingMixin):
    """
    GCSTaskHandler is a python log handler that handles and reads task instance logs.

    It extends airflow FileTaskHandler and uploads to and reads from GCS remote
    storage. Upon log reading failure, it reads from host machine's local disk.

    :param base_log_folder: Base log folder to place logs.
    :param gcs_log_folder: Path to a remote location where logs will be saved. It must have the prefix
        ``gs://``. For example: ``gs://bucket/remote/log/location``
    :param filename_template: template filename string
    :param gcp_key_path: Path to Google Cloud Service Account file (JSON). Mutually exclusive with
        gcp_keyfile_dict.
        If omitted, authorization based on `the Application Default Credentials
        <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__ will
        be used.
    :param gcp_keyfile_dict: Dictionary of keyfile parameters. Mutually exclusive with gcp_key_path.
    :param gcp_scopes: Comma-separated string containing OAuth2 scopes
    :param project_id: Project ID to read the secrets from. If not passed, the project ID from credentials
        will be used.
    :param delete_local_copy: Whether local log files should be deleted after they are downloaded when using
        remote logging
    """

    trigger_should_wrap = True

    def __init__(
        self,
        *,
        base_log_folder: str,
        gcs_log_folder: str,
        gcp_key_path: str | None = None,
        gcp_keyfile_dict: dict | None = None,
        gcp_scopes: Collection[str] | None = _DEFAULT_SCOPESS,
        project_id: str = PROVIDE_PROJECT_ID,
        max_bytes: int = 0,
        backup_count: int = 0,
        delay: bool = False,
        **kwargs,
    ) -> None:
        # support log file size handling of FileTaskHandler
        super().__init__(
            base_log_folder=base_log_folder, max_bytes=max_bytes, backup_count=backup_count, delay=delay
        )
        self.handler: logging.FileHandler | None = None
        self.log_relative_path = ""
        self.closed = False
        self.upload_on_close = True
        self.io = GCSRemoteLogIO(
            base_log_folder=base_log_folder,
            remote_base=gcs_log_folder,
            delete_local_copy=kwargs.get(
                "delete_local_copy", conf.getboolean("logging", "delete_local_logs")
            ),
            gcp_key_path=gcp_key_path,
            gcp_keyfile_dict=gcp_keyfile_dict,
            scopes=gcp_scopes,
            project_id=project_id,
        )

    def set_context(self, ti: TaskInstance, *, identifier: str | None = None) -> None:
        super().set_context(ti, identifier=identifier)
        # Log relative path is used to construct local and remote
        # log path to upload log files into GCS and read from the
        # remote location.
        if TYPE_CHECKING:
            assert self.handler is not None

        self.ti = ti

        full_path = self.handler.baseFilename
        self.log_relative_path = Path(full_path).relative_to(self.local_base).as_posix()
        is_trigger_log_context = getattr(ti, "is_trigger_log_context", False)
        self.upload_on_close = is_trigger_log_context or not getattr(ti, "raw", None)

    def close(self):
        """Close and upload local log file to remote storage GCS."""
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        super().close()

        if not self.upload_on_close:
            return

        if hasattr(self, "ti"):
            self.io.upload(self.log_relative_path, self.ti)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _read_remote_logs(self, ti, try_number, metadata=None) -> LogResponse:
        # Explicitly getting log relative path is necessary as the given
        # task instance might be different than task instance passed in
        # in set_context method.
        worker_log_rel_path = self._render_filename(ti, try_number)

        messages, logs = self.io.read(worker_log_rel_path, ti)

        if logs is None:
            logs = []
            if not AIRFLOW_V_3_0_PLUS and not messages:
                messages.append(f"No logs found in GCS; ti={ti}")

        return messages, logs
