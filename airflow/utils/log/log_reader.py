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
import time
from functools import cached_property
from typing import Iterator

from sqlalchemy.orm.session import Session

from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance
from airflow.utils.helpers import render_log_filename
from airflow.utils.log.logging_mixin import ExternalLoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState


class TaskLogReader:
    """Task log reader."""

    STREAM_LOOP_SLEEP_SECONDS = 0.5
    """Time to sleep between loops while waiting for more logs"""

    def read_log_chunks(
        self, ti: TaskInstance, try_number: int | None, metadata
    ) -> tuple[list[tuple[tuple[str, str]]], dict[str, str]]:
        """
        Read chunks of Task Instance logs.

        :param ti: The taskInstance
        :param try_number: If provided, logs for the given try will be returned.
            Otherwise, logs from all attempts are returned.
        :param metadata: A dictionary containing information about how to read the task log

        The following is an example of how to use this method to read log:

        .. code-block:: python

            logs, metadata = task_log_reader.read_log_chunks(ti, try_number, metadata)
            logs = logs[0] if try_number is not None else logs

        where task_log_reader is an instance of TaskLogReader. The metadata will always
        contain information about the task log which can enable you read logs to the
        end.
        """
        logs, metadatas = self.log_handler.read(ti, try_number, metadata=metadata)
        metadata = metadatas[0]
        return logs, metadata

    def read_log_stream(self, ti: TaskInstance, try_number: int | None, metadata: dict) -> Iterator[str]:
        """
        Continuously read log to the end.

        :param ti: The Task Instance
        :param try_number: the task try number
        :param metadata: A dictionary containing information about how to read the task log
        """
        if try_number is None:
            next_try = ti.next_try_number
            try_numbers = list(range(1, next_try))
        else:
            try_numbers = [try_number]
        for current_try_number in try_numbers:
            metadata.pop("end_of_log", None)
            metadata.pop("max_offset", None)
            metadata.pop("offset", None)
            metadata.pop("log_pos", None)
            while True:
                logs, metadata = self.read_log_chunks(ti, current_try_number, metadata)
                for host, log in logs[0]:
                    yield "\n".join([host or "", log]) + "\n"
                if "end_of_log" not in metadata or (
                    not metadata["end_of_log"]
                    and ti.state not in (TaskInstanceState.RUNNING, TaskInstanceState.DEFERRED)
                ):
                    if not logs[0]:
                        # we did not receive any logs in this loop
                        # sleeping to conserve resources / limit requests on external services
                        time.sleep(self.STREAM_LOOP_SLEEP_SECONDS)
                else:
                    break

    @cached_property
    def log_handler(self):
        """Get the log handler which is configured to read logs."""
        task_log_reader = conf.get("logging", "task_log_reader")

        def handlers():
            """
            Yield all handlers first from airflow.task logger then root logger.

            Depending on whether we're in a running task, it could be in either of these locations.
            """
            yield from logging.getLogger("airflow.task").handlers
            yield from logging.getLogger().handlers

        return next((h for h in handlers() if h.name == task_log_reader), None)

    @property
    def supports_read(self):
        """Checks if a read operation is supported by a current log handler."""
        return hasattr(self.log_handler, "read")

    @property
    def supports_external_link(self) -> bool:
        """Check if the logging handler supports external links (e.g. to Elasticsearch, Stackdriver, etc)."""
        if not isinstance(self.log_handler, ExternalLoggingMixin):
            return False

        return self.log_handler.supports_external_link

    @provide_session
    def render_log_filename(
        self,
        ti: TaskInstance,
        try_number: int | None = None,
        *,
        session: Session = NEW_SESSION,
    ) -> str:
        """
        Render the log attachment filename.

        :param ti: The task instance
        :param try_number: The task try number
        """
        dagrun = ti.get_dagrun(session=session)
        attachment_filename = render_log_filename(
            ti=ti,
            try_number="all" if try_number is None else try_number,
            filename_template=dagrun.get_log_template(session=session).filename,
        )
        return attachment_filename
