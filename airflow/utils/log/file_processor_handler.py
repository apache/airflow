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

import errno
import logging
import os

from airflow import settings
from airflow.utils.helpers import parse_template_string
from datetime import datetime


class FileProcessorHandler(logging.Handler):
    """
    FileProcessorHandler is a python log handler that handles
    dag processor logs. It creates and delegates log handling
    to `logging.FileHandler` after receiving dag processor context.
    """

    def __init__(self, base_log_folder, filename_template):
        """
        :param base_log_folder: Base log folder to place logs.
        :param filename_template: template filename string
        """
        super(FileProcessorHandler, self).__init__()
        self.handler = None
        self.base_log_folder = base_log_folder
        self.dag_dir = os.path.expanduser(settings.DAGS_FOLDER)
        self.filename_template, self.filename_jinja_template = \
            parse_template_string(filename_template)
        self._cur_date = None

    def set_context(self, filename):
        """
        Provide filename context to airflow task handler.
        :param filename: filename in which the dag is located
        """
        log_directory = os.path.join(self.base_log_folder,
                                     datetime.utcnow().strftime("%Y-%m-%d"))
        if not os.path.exists(log_directory):
            try:
                os.makedirs(log_directory)
            except OSError as e:
                # only ignore case where the directory already exist
                if e.errno != errno.EEXIST:
                    raise
                logging.warning("%s already exists", log_directory)

        local_loc = self._init_file(filename, log_directory)
        self.handler = logging.FileHandler(local_loc)
        self.handler.setFormatter(self.formatter)
        self.handler.setLevel(self.level)
        if self._cur_date is None or self._cur_date < datetime.today():
            self._symlink_latest_log_directory(log_directory)
            self._cur_date = datetime.today()

    def emit(self, record):
        if self.handler is not None:
            self.handler.emit(record)

    def flush(self):
        if self.handler is not None:
            self.handler.flush()

    def close(self):
        if self.handler is not None:
            self.handler.close()

    def _render_filename(self, filename):
        filename = os.path.relpath(filename, self.dag_dir)
        ctx = dict()
        ctx['filename'] = filename

        if self.filename_jinja_template:
            return self.filename_jinja_template.render(**ctx)

        return self.filename_template.format(filename=ctx['filename'])

    def _symlink_latest_log_directory(self, log_directory):
        latest_log_directory_path = os.path.join(self.base_log_folder, "latest")
        try:
            # if symlink exists but is stale, update it
            if os.path.islink(latest_log_directory_path):
                if os.readlink(latest_log_directory_path) != log_directory:
                    os.unlink(latest_log_directory_path)
                    os.symlink(log_directory, latest_log_directory_path)
            elif (os.path.isdir(latest_log_directory_path) or
                  os.path.isfile(latest_log_directory_path)):
                logging.warning(
                    "%s already exists as a dir/file. Skip creating symlink.",
                    latest_log_directory_path
                )
            else:
                os.symlink(log_directory, latest_log_directory_path)
        except OSError:
            logging.warning("OSError while attempting to symlink "
                            "the latest log directory")

    def _init_file(self, filename, log_directory):
        full_path = os.path.join(log_directory, self._render_filename(filename))
        directory = os.path.dirname(full_path)

        if not os.path.exists(directory):
            os.makedirs(directory)

        if not os.path.exists(full_path):
            open(full_path, "a").close()

        return full_path
