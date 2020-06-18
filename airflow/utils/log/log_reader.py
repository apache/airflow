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

import logging
from typing import Any, Dict, List, Optional, Tuple

from cached_property import cached_property

from airflow.configuration import conf
from airflow.models import TaskInstance
from airflow.utils.helpers import render_log_filename


class TaskLogReader:
    """ Task log reader"""

    def read_log_chunks(self, ti: TaskInstance, try_number: Optional[int],
                        metadata) -> Tuple[List[str], Dict[str, Any]]:
        """
        Reads chunks of Task Instance logs.

        :param ti: The taskInstance
        :type ti: TaskInstance
        :param try_number: If provided, logs for the given try will be returned. Otherwise, logs from all attempts are returned.
        :type try_number: Optional[int]
        :param metadata: A dictionary containing information about how to read the task
        :type metadata: dict

        """

        logs, metadatas = self.log_handler.read(ti, try_number, metadata=metadata)
        metadata = metadatas[0]
        return logs, metadata

    def read_log_stream(self, ti: TaskInstance, try_number: Optional[int], metadata: dict):
        """
        Used to continuously read log to the end

        :param ti: The Task Instance
        :type ti: TaskInstance
        :param try_number: the task try number
        :type try_number: Optional[int]
        :param metadata: A dictionary containing information about how to read the task
        :type metadata: dict

        """

        if try_number is None:
            next_try = ti.next_try_number
            try_numbers = list(range(1, next_try))
        else:
            try_numbers = [try_number]
        for current_try_number in try_numbers:
            metadata.pop('end_of_log', None)
            metadata.pop('max_offset', None)
            metadata.pop('offset', None)
            while 'end_of_log' not in metadata or not metadata['end_of_log']:
                logs, metadata = self.read_log_chunks(ti, current_try_number, metadata)
                yield "\n".join(logs) + "\n"

    @cached_property
    def log_handler(self):
        """Log handler, which is configured to read logs."""

        logger = logging.getLogger('airflow.task')
        task_log_reader = conf.get('logging', 'task_log_reader')
        handler = next((handler for handler in logger.handlers if handler.name == task_log_reader), None)
        return handler

    @property
    def is_supported(self):
        """Checks if a read operation is supported by a current log handler."""

        return hasattr(self.log_handler, 'read')

    def render_log_filename(self, ti: TaskInstance, try_number: Optional[int] = None):
        """
        Renders the log attachment filename

        :param ti: The task instance
        :type ti: TaskInstance
        :param try_number: The task try number
        :type try_number: Optional[int]
        """

        filename_template = conf.get('logging', 'LOG_FILENAME_TEMPLATE')
        attachment_filename = render_log_filename(
            ti=ti,
            try_number="all" if try_number is None else try_number,
            filename_template=filename_template)
        return attachment_filename
