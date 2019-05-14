# -*- coding: utf-8 -*-
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

# Using `from elasticsearch import *` would break elasticsearch mocking used in unit test.
import elasticsearch
import pendulum
from elasticsearch_dsl import Search

from airflow.utils import timezone
from airflow.utils.helpers import parse_template_string
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin


class ElasticsearchTaskHandler(FileTaskHandler, LoggingMixin):
    PAGE = 0
    MAX_LINE_PER_PAGE = 1000

    """
    ElasticsearchTaskHandler is a python log handler that
    reads logs from Elasticsearch. Note logs are not directly
    indexed into Elasticsearch. Instead, it flushes logs
    into local files. Additional software setup is required
    to index the log into Elasticsearch, such as using
    Filebeat and Logstash.
    To efficiently query and sort Elasticsearch results, we assume each
    log message has a field `log_id` consists of ti primary keys:
    `log_id = {dag_id}-{task_id}-{execution_date}-{try_number}`
    Log messages with specific log_id are sorted based on `offset`,
    which is a unique integer indicates log message's order.
    Timestamp here are unreliable because multiple log messages
    might have the same timestamp.
    """

    def __init__(self, base_log_folder, filename_template,
                 log_id_template, end_of_log_mark,
                 host='localhost:9200'):
        """
        :param base_log_folder: base folder to store logs locally
        :param log_id_template: log id template
        :param host: Elasticsearch host name
        """
        super().__init__(
            base_log_folder, filename_template)
        self.closed = False

        self.log_id_template, self.log_id_jinja_template = \
            parse_template_string(log_id_template)

        self.client = elasticsearch.Elasticsearch([host])

        self.mark_end_on_close = True
        self.end_of_log_mark = end_of_log_mark

    def _render_log_id(self, ti, try_number):
        if self.log_id_jinja_template:
            jinja_context = ti.get_template_context()
            jinja_context['try_number'] = try_number
            return self.log_id_jinja_template.render(**jinja_context)

        return self.log_id_template.format(dag_id=ti.dag_id,
                                           task_id=ti.task_id,
                                           execution_date=ti
                                           .execution_date.isoformat(),
                                           try_number=try_number)

    def _read(self, ti, try_number, metadata=None):
        """
        Endpoint for streaming log.
        :param ti: task instance object
        :param try_number: try_number of the task instance
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        :return: a list of log documents and metadata.
        """
        if not metadata:
            metadata = {'offset': 0}
        if 'offset' not in metadata:
            metadata['offset'] = 0

        offset = metadata['offset']
        log_id = self._render_log_id(ti, try_number)

        logs = self.es_read(log_id, offset, metadata)

        next_offset = offset if not logs else logs[-1].offset

        metadata['offset'] = next_offset
        # end_of_log_mark may contain characters like '\n' which is needed to
        # have the log uploaded but will not be stored in elasticsearch.
        metadata['end_of_log'] = False if not logs \
            else logs[-1].message == self.end_of_log_mark.strip()

        cur_ts = pendulum.now()
        # Assume end of log after not receiving new log for 5 min,
        # as executor heartbeat is 1 min and there might be some
        # delay before Elasticsearch makes the log available.
        if 'last_log_timestamp' in metadata:
            last_log_ts = timezone.parse(metadata['last_log_timestamp'])
            if cur_ts.diff(last_log_ts).in_minutes() >= 5 or 'max_offset' in metadata \
                    and offset >= metadata['max_offset']:
                metadata['end_of_log'] = True

        if offset != next_offset or 'last_log_timestamp' not in metadata:
            metadata['last_log_timestamp'] = str(cur_ts)

        message = '\n'.join([log.message for log in logs])

        return message, metadata

    def es_read(self, log_id, offset, metadata):
        """
        Returns the logs matching log_id in Elasticsearch and next offset.
        Returns '' if no log is found or there was an error.
        :param log_id: the log_id of the log to read.
        :type log_id: str
        :param offset: the offset start to read log from.
        :type offset: str
        :param metadata: log metadata, used for steaming log download.
        :type metadata: dict
        """

        # Offset is the unique key for sorting logs given log_id.
        s = Search(using=self.client) \
            .query('match_phrase', log_id=log_id) \
            .sort('offset')

        s = s.filter('range', offset={'gt': offset})
        max_log_line = s.count()
        if 'download_logs' in metadata and metadata['download_logs'] and 'max_offset' not in metadata:
            try:
                metadata['max_offset'] = s[max_log_line - 1].execute()[-1].offset if max_log_line > 0 else 0
            except Exception:
                self.log.exception('Could not get current log size with log_id: {}'.format(log_id))

        logs = []
        if max_log_line != 0:
            try:

                logs = s[self.MAX_LINE_PER_PAGE * self.PAGE:self.MAX_LINE_PER_PAGE] \
                    .execute()
            except Exception as e:
                self.log.exception('Could not read log with log_id: %s, error: %s', log_id, str(e))

        return logs

    def set_context(self, ti):
        super().set_context(ti)
        self.mark_end_on_close = not ti.raw

    def close(self):
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        if not self.mark_end_on_close:
            self.closed = True
            return

        # Case which context of the handler was not set.
        if self.handler is None:
            self.closed = True
            return

        # Reopen the file stream, because FileHandler.close() would be called
        # first in logging.shutdown() and the stream in it would be set to None.
        if self.handler.stream is None or self.handler.stream.closed:
            self.handler.stream = self.handler._open()

        # Mark the end of file using end of log mark,
        # so we know where to stop while auto-tailing.
        self.handler.stream.write(self.end_of_log_mark)

        super().close()

        self.closed = True
