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

# Using `from elasticsearch import *` breaks es mocking in unit test.
import elasticsearch
<<<<<<< HEAD
<<<<<<< HEAD
import json
import logging
=======
import io
import json
import logging
import os
import requests
>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
import json
import logging
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings
import sys

import pendulum
from elasticsearch_dsl import Search

from airflow.utils import timezone
from airflow.utils.helpers import parse_template_string
from airflow.utils.log.file_task_handler import FileTaskHandler
<<<<<<< HEAD


class ParentStdout():
    """
    Keep track of the ParentStdout stdout context in child process
    """
    def __init__(self):
        self.closed = False

    def write(self, string):
        sys.__stdout__.write(string)

    def close(self):
        self.closed = True


class JsonFormatter(logging.Formatter):
    """
    Custom formatter to allow for fields to be captured in JSON format.
    Fields are added via the RECORD_LABELS list.
    TODO: Move RECORD_LABELS into configs/log_config.py
    """
    def __init__(self, record_labels, processedTask=None):
        super(JsonFormatter, self).__init__()
        self.processedTask = processedTask
        self.record_labels = record_labels

    def format(self, record):
        recordObj = {label: getattr(record, label)
                     for label in self.record_labels}
        recordObj = {**recordObj, **self.processedTask}
        return json.dumps(recordObj)


class ParentStdout():
    """
    Keep track of the ParentStdout stdout context in child process
    """
    def __init__(self):
        self.closed = False

    def write(self, string):
        sys.__stdout__.write(string)

    def close(self):
        self.closed = True


class JsonFormatter(logging.Formatter):
    """
    Custom formatter to allow for fields to be captured in JSON format.
    Fields are added via the RECORD_LABELS list.
    TODO: Move RECORD_LABELS into configs/log_config.py
    """
    def __init__(self, record_labels, processedTask=None):
        super(JsonFormatter, self).__init__()
        self.processedTask = processedTask
        self.record_labels = record_labels

    def format(self, record):
        recordObj = {label: getattr(record, label)
                     for label in self.record_labels}
        recordObj = {**recordObj, **self.processedTask}
        return json.dumps(recordObj)
=======
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings


class ParentStdout():
    """
    Keep track of the ParentStdout stdout context in child process
    """
    def __init__(self):
        self.closed = False

    def write(self, string):
        sys.__stdout__.write(string)


class JsonFormatter(logging.Formatter):
    """
    Custom formatter to allow for fields to be captured in JSON format.
    Fields are added via the RECORD_LABELS list.
    TODO: Move RECORD_LABELS into configs/log_config.py
    """
    def __init__(self, record_labels, processedTask=None):
        super(JsonFormatter, self).__init__()
        self.processedTask = processedTask
        self.record_labels = record_labels

    def format(self, record):
        recordObj = {label: getattr(record, label)
                     for label in self.record_labels}
        recordObj = {**recordObj, **self.processedTask}
        return json.dumps(recordObj)


class ElasticsearchTaskHandler(FileTaskHandler):
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
                 write_stdout=None, json_format=None,
                 record_labels=None, host='localhost:9200'):
=======
                 host='localhost:9200', write_stdout):
>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
                 write_stdout, host='localhost:9200'):
>>>>>>> fc278da4... [AIRFLOW-3370] Fix class name, reformat ENV conditionals, add string replacements for log_id
=======
                 write_stdout, json_format, host='localhost:9200'):
>>>>>>> 42735a2b... [AIRFLOW-3370] Add in var for JSON formatting and add option through write_stdout
=======
                 write_stdout, json_format, record_labels, host=None):
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings
        """
        :param base_log_folder: base folder to store logs locally
        :param log_id_template: log id template
        :param host: Elasticsearch host name
        """
        super(ElasticsearchTaskHandler, self).__init__(
            base_log_folder, filename_template)

        self.closed = False
        self.write_stdout = write_stdout
<<<<<<< HEAD
<<<<<<< HEAD
        self.json_format = json_format
        self.record_labels = record_labels
=======
>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
        self.json_format = json_format
<<<<<<< HEAD
>>>>>>> 42735a2b... [AIRFLOW-3370] Add in var for JSON formatting and add option through write_stdout
=======
        self.record_labels = record_labels
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings

        self.handler = None
        self.taskInstance = None
        self.writer = None

        self.log_id_template, self.log_id_jinja_template = \
            parse_template_string(log_id_template)

        self.client = elasticsearch.Elasticsearch([host])

        self.mark_end_on_close = True
        self.end_of_log_mark = end_of_log_mark

    def set_context(self, ti):
<<<<<<< HEAD
<<<<<<< HEAD
        if self.write_stdout:
            self.writer = ParentStdout()
            sys.stdout = self.writer

            self.taskInstance = self._process_task_instance(ti)

            self.handler = logging.StreamHandler(stream=sys.stdout)

            if self.json_format:
                self.handler.setFormatter(JsonFormatter(self.record_labels,
                                                        self.taskInstance))
            elif not self.json_format:
                self.handler.setFormatter(self.formatter)
            self.handler.setLevel(self.level)

        elif not self.write_stdout:
=======
        if self.write_stdout == True:
            self.writer = parentStdout()
=======
        if self.write_stdout:
            self.writer = ParentStdout()
>>>>>>> fc278da4... [AIRFLOW-3370] Fix class name, reformat ENV conditionals, add string replacements for log_id
            sys.stdout = self.writer

            self.taskInstance = self._process_task_instance(ti)

            self.handler = logging.StreamHandler(stream=sys.stdout)

            if self.json_format:
                self.handler.setFormatter(JsonFormatter(self.taskInstance))
            elif not self.json_format:
                self.handler.setFormatter(self.formatter)
            self.handler.setLevel(self.level)
<<<<<<< HEAD
<<<<<<< HEAD
        elif self.write_stdout == False:
>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
=======

>>>>>>> 42735a2b... [AIRFLOW-3370] Add in var for JSON formatting and add option through write_stdout
        elif not self.write_stdout:
>>>>>>> fc278da4... [AIRFLOW-3370] Fix class name, reformat ENV conditionals, add string replacements for log_id
            super(ElasticsearchTaskHandler, self).set_context(ti)
        self.mark_end_on_close = not ti.raw

    def emit(self, record):
<<<<<<< HEAD
<<<<<<< HEAD
        if self.write_stdout:
=======
        if self.write_stdout == True:
>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
        if self.write_stdout:
>>>>>>> fc278da4... [AIRFLOW-3370] Fix class name, reformat ENV conditionals, add string replacements for log_id
            self.formatter.format(record)
            if self.handler is not None:
                self.handler.emit(record)
        elif not self.write_stdout:
            super(ElasticsearchTaskHandler).emit(record)

    def flush(self):
        if self.handler is not None:
            self.handler.flush()

<<<<<<< HEAD
<<<<<<< HEAD
    def _process_task_instance(self, ti):
        ti_info = {'dag_id': str(ti.dag_id),
                   'task_id': str(ti.task_id),
                   'execution_date': str(ti.execution_date),
                   'try_number': str(ti.try_number)}
<<<<<<< HEAD
=======
    def _process_taskInstance(self, ti):
=======
    def _process_task_instance(self, ti):
>>>>>>> 23e0518e... [AIRFLOW-3370] Switch to snakecase
        ti_info =  {'dag_id': str(ti.dag_id),
                    'task_id': str(ti.task_id),
                    'execution_date': str(ti.execution_date),
                    'try_number': str(ti.try_number)}
>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings
        return ti_info

    def read(self, task_instance, try_number=None, metadata=None):
            """
            Read logs of a given task instance from elasticsearch.
            :param task_instance: task instance object
<<<<<<< HEAD
<<<<<<< HEAD
            :param try_number: task instance try_number to read logs from.
            If None,it will start at 1.
            """
            if self.write_stdout:
=======
            :param try_number: task instance try_number to read logs from. If None,
                               it will start at 1.
=======
            :param try_number: task instance try_number to read logs from.
            If None,it will start at 1.
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings
            """
<<<<<<< HEAD
            if self.write_stdout == True:
>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
            if self.write_stdout:
>>>>>>> fc278da4... [AIRFLOW-3370] Fix class name, reformat ENV conditionals, add string replacements for log_id
                if try_number is None:
                    next_try = task_instance.next_try_number
                    try_numbers = list(range(1, next_try))
                elif try_number < 1:
                    logs = [
<<<<<<< HEAD
<<<<<<< HEAD
                        'Error fetching the logs. \
                         Try number {} is invalid.'.format(try_number),
=======
                        'Error fetching the logs. Try number {} is invalid.'.format(try_number),
>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
                        'Error fetching the logs. \
                         Try number {} is invalid.'.format(try_number),
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings
                    ]
                    return logs
                else:
                    try_numbers = [try_number]

                logs = [''] * len(try_numbers)
                metadatas = [{}] * len(try_numbers)
                for i, try_number in enumerate(try_numbers):
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings
                    log, metadata = self._read(task_instance,
                                               try_number,
                                               metadata)

                    # If there's a log present, then we don't want to keep
                    # checking. Set end_of_log to True, set the
                    # mark_end_on_close to False and return the log and
                    # metadata. This will prevent the recursion from happening
                    # in the ti_log.html script and will therefore prevent
                    # constantly checking ES for updates, since we've
<<<<<<< HEAD
=======
                    log, metadata = self._read(task_instance, try_number, metadata)

                    # If there's a log present, then we don't want to keep checking. Set end_of_log
                    # to True, set the mark_end_on_close to False and return the log and metadata
                    # This will prevent the recursion from happening in the ti_log.html script
                    # and will therefore prevent constantly checking ES for updates, since we've
>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings
                    # fetched what we're looking for
                    if log:
                        logs[i] += log
                        metadata['end_of_log'] = True
                        self.mark_end_on_close = False
                        metadatas[i] = metadata
<<<<<<< HEAD
                    elif not log:
                        metadata['end_of_log'] = False
                        metadatas[i] = metadata

                return logs, metadatas
            elif not self.write_stdout:
                return super(ElasticsearchTaskHandler, self).read(task_instance,
                                                                  try_number,
                                                                  metadata)
=======

                return logs, metadatas
            elif not self.write_stdout:
<<<<<<< HEAD
                super(ElasticsearchTaskHandler, self).read(task_instance, try_number, metadata)
>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
                super(ElasticsearchTaskHandler, self).read(task_instance,
                                                           try_number,
                                                           metadata)
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings

    def _render_log_id(self, ti, try_number):
        # Using Jinja2 templating
        if self.log_id_jinja_template:
            jinja_context = ti.get_template_context()
            jinja_context['try_number'] = try_number
            return self.log_id_jinja_template.render(**jinja_context)

        # Make log_id ES Query friendly if using standard out option
        if self.write_stdout:
            return self.log_id_template.format(dag_id=ti.dag_id,
<<<<<<< HEAD
<<<<<<< HEAD
                                               task_id=ti.task_id,
                                               execution_date=ti
                                               .execution_date.isoformat(),
                                               try_number=try_number) \
                                               .replace(":", "_") \
                                               .replace("-", "_") \
                                               .replace("+", "_")

        return self.log_id_template.format(dag_id=ti.dag_id,
                                               task_id=ti.task_id,
                                               execution_date=ti
                                               .execution_date.isoformat(),
                                               try_number=try_number) \
                                               .replace(":", "_") \
                                               .replace("-", "_") \
                                               .replace("+", "_")
=======
                                                   task_id=ti.task_id,
                                                   execution_date=ti
                                                   .execution_date.isoformat(),
                                                   try_number=try_number).replace(":", "_").replace("-", "_").replace("+", "_")
<<<<<<< HEAD

>>>>>>> fc278da4... [AIRFLOW-3370] Fix class name, reformat ENV conditionals, add string replacements for log_id
=======
>>>>>>> 42735a2b... [AIRFLOW-3370] Add in var for JSON formatting and add option through write_stdout
        return self.log_id_template.format(dag_id=ti.dag_id,
=======
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings
                                               task_id=ti.task_id,
                                               execution_date=ti
                                               .execution_date.isoformat(),
                                               try_number=try_number) \
                                               .replace(":", "_") \
                                               .replace("-", "_") \
                                               .replace("+", "_")
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
        :return a list of log documents and metadata.
        """
        if not metadata:
            metadata = {'offset': 0}
        if 'offset' not in metadata:
            metadata['offset'] = 0

        offset = metadata['offset']
        log_id = self._render_log_id(ti, try_number)

        logs = self.es_read(log_id, offset)

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
            if cur_ts.diff(last_log_ts).in_minutes() >= 5:
                metadata['end_of_log'] = True

        if offset != next_offset or 'last_log_timestamp' not in metadata:
            metadata['last_log_timestamp'] = str(cur_ts)

        message = '\n'.join([log.message for log in logs])

        return message, metadata

    def es_read(self, log_id, offset):
        """
        Returns the logs matching log_id in Elasticsearch and next offset.
        Returns '' if no log is found or there was an error.
        :param log_id: the log_id of the log to read.
        :type log_id: str
        :param offset: the offset start to read log from.
        :type offset: str
        """
<<<<<<< HEAD
<<<<<<< HEAD
=======

        log_components = log_id.split("-")
        dag_id = log_components[0]
        task_id = log_components[1]
        execution_date = '_'.join(log_components[2:5]).replace(":", "_").replace("-", "_").replace("+", "_")
        try_number = str(int(log_components[5]))

        new_log_id = f"{dag_id}_{task_id}_{execution_date}_{try_number}"

>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
>>>>>>> fc278da4... [AIRFLOW-3370] Fix class name, reformat ENV conditionals, add string replacements for log_id
        # Offset is the unique key for sorting logs given log_id.
        s = Search(using=self.client) \
            .query('match', log_id=log_id) \
            .sort('offset')

        s = s.filter('range', offset={'gt': offset})

        logs = []
        if s.count() != 0:
            try:
                logs = s[self.MAX_LINE_PER_PAGE *
                         self.PAGE:self.MAX_LINE_PER_PAGE] \
                    .execute()
            except Exception as e:
                msg = 'Could not read log with log_id: {}, ' \
                      'error: {}'.format(log_id, str(e))
                self.log.exception(msg)

        return logs

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
        # first in logging.shutdown() and the stream in it would be set to None
<<<<<<< HEAD
        if not self.write_stdout and (self.handler.stream is None or self.handler.stream.closed):
=======
        if self.handler.stream is None or self.handler.stream.closed:
>>>>>>> d98bcb1c... [AIRFLOW-3370] Move RECORD_LABELS to log_config.py custom log config fileand fix flake8 linting errors + warnings
            self.handler.stream = self.handler._open()

        # Mark the end of file using end of log mark,
        # so we know where to stop while auto-tailing.
<<<<<<< HEAD
        # Don't need to do this if using write_stdout, this is handled in read
        if not self.write_stdout:
            self.handler.stream.write(self.end_of_log_mark)

        if self.write_stdout:
            if self.handler is not None:
                self.writer.close()
                self.handler.close()
                sys.stdout = sys.__stdout__

        elif not self.write_stdout:
=======
        # self.handler.stream.write(self.end_of_log_mark)

        if self.write_stdout:
            if self.handler is not None:
                self.writer.closed = True
                self.handler.close()
                sys.stdout = sys.__stdout__

<<<<<<< HEAD
        if not self.write_stdout:
>>>>>>> 31f18134... [AIRFLOW-3370] Add stdout and JSON functionality to ES task handler
=======
        elif not self.write_stdout:
>>>>>>> fc278da4... [AIRFLOW-3370] Fix class name, reformat ENV conditionals, add string replacements for log_id
            super(ElasticsearchTaskHandler, self).close()

        self.closed = True
