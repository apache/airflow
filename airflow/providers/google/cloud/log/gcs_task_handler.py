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
from pathlib import Path
from typing import Collection

# not sure why but mypy complains on missing `storage` but it is clearly there and is importable
from google.cloud import storage  # type: ignore[attr-defined]

from airflow.compat.functools import cached_property
from airflow.configuration import conf
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.providers.google.cloud.utils.credentials_provider import get_credentials_and_project_id
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin

_DEFAULT_SCOPESS = frozenset(
    [
        "https://www.googleapis.com/auth/devstorage.read_write",
    ]
)

logger = logging.getLogger(__name__)


class GCSTaskHandler(FileTaskHandler, LoggingMixin):
    """
    GCSTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow FileTaskHandler and
    uploads to and reads from GCS remote storage. Upon log reading
    failure, it reads from host machine's local disk.

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
    """

    trigger_should_wrap = True

    def __init__(
        self,
        *,
        base_log_folder: str,
        gcs_log_folder: str,
        filename_template: str | None = None,
        gcp_key_path: str | None = None,
        gcp_keyfile_dict: dict | None = None,
        gcp_scopes: Collection[str] | None = _DEFAULT_SCOPESS,
        project_id: str | None = None,
    ):
        super().__init__(base_log_folder, filename_template)
        self.remote_base = gcs_log_folder
        self.log_relative_path = ""
        self.closed = False
        self.upload_on_close = True
        self.gcp_key_path = gcp_key_path
        self.gcp_keyfile_dict = gcp_keyfile_dict
        self.scopes = gcp_scopes
        self.project_id = project_id

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

    def set_context(self, ti):
        super().set_context(ti)
        # Log relative path is used to construct local and remote
        # log path to upload log files into GCS and read from the
        # remote location.
        full_path = self.handler.baseFilename
        self.log_relative_path = Path(full_path).relative_to(self.local_base).as_posix()
        is_trigger_log_context = getattr(ti, "is_trigger_log_context", False)
        self.upload_on_close = is_trigger_log_context or not ti.raw

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

        local_loc = os.path.join(self.local_base, self.log_relative_path)
        remote_loc = os.path.join(self.remote_base, self.log_relative_path)
        if os.path.exists(local_loc):
            # read log and remove old logs to get just the latest additions
            with open(local_loc) as logfile:
                log = logfile.read()
            self.gcs_write(log, remote_loc)

        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def _add_message(self, msg):
        filename, lineno, func, stackinfo = logger.findCaller()
        record = logging.LogRecord("", logging.INFO, filename, lineno, msg + "\n", None, None, func=func)
        return self.format(record)

    def _read_remote_logs(self, ti, try_number, metadata=None) -> tuple[list[str], list[str]]:
        # Explicitly getting log relative path is necessary because this method
        # is called from webserver from TaskLogReader, where we don't call set_context
        # and can read logs for different TIs in each request
        messages = []
        logs = []
        worker_log_relative_path = self._render_filename(ti, try_number)
        remote_loc = os.path.join(self.remote_base, worker_log_relative_path)
        uris = []
        bucket, prefix = _parse_gcs_url(remote_loc)
        blobs = list(self.client.list_blobs(bucket_or_name=bucket, prefix=prefix))

        if blobs:
            uris = [f"gs://{bucket}/{b.name}" for b in blobs]
            messages.extend(["Found remote logs:", *[f"  * {x}" for x in sorted(uris)]])
        else:
            messages.append(f"No logs found in GCS; ti=%s {ti}")
        try:
            for key in sorted(uris):
                blob = storage.Blob.from_string(key, self.client)
                remote_log = blob.download_as_bytes().decode()
                if remote_log:
                    logs.append(remote_log)
        except Exception as e:
            messages.append(f"Unable to read remote log {e}")
        return messages, logs

    def _read(self, ti, try_number, metadata=None):
        """
        Read logs of given task instance and try_number from GCS.
        If failed, read the log from task instance host machine.

        todo: when min airflow version >= 2.6, remove this method

        :param ti: task instance object
        :param try_number: task instance try_number to read logs from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        """
        if hasattr(super(), "_read_remote_logs"):
            # from Airflow 2.6, we don't implement the `_read` method.
            # if parent has _read_remote_logs, we're >= 2.6
            return super()._read(ti, try_number, metadata)

        messages, logs = self._read_remote_logs(ti, try_number, metadata)
        if not logs:
            return super()._read(ti, try_number, metadata)

        return "".join([f"*** {x}\n" for x in messages]) + "\n".join(logs), {"end_of_log": True}

    def gcs_write(self, log, remote_log_location):
        """
        Writes the log to the remote_log_location. Fails silently if no log
        was created.

        :param log: the log to write to the remote_log_location
        :param remote_log_location: the log's location in remote storage
        """
        try:
            blob = storage.Blob.from_string(remote_log_location, self.client)
            old_log = blob.download_as_bytes().decode()
            log = "\n".join([old_log, log]) if old_log else log
        except Exception as e:
            if self.no_log_found(e):
                pass
            else:
                log += self._add_message(
                    f"Error checking for previous log; if exists, may be overwritten: {str(e)}"
                )
                self.log.warning("Error checking for previous log: %s", e)
        try:
            blob = storage.Blob.from_string(remote_log_location, self.client)
            blob.upload_from_string(log, content_type="text/plain")
        except Exception as e:
            self.log.error("Could not write logs to %s: %s", remote_log_location, e)

    @staticmethod
    def no_log_found(exc):
        """
        Given exception, determine whether it is result of log not found.

        :meta private:
        """
        if exc.args and isinstance(exc.args[0], str) and "No such object" in exc.args[0]:
            return True
        elif getattr(exc, "resp", {}).get("status") == "404":
            return True
        return False
