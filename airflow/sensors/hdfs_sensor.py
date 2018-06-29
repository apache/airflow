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

import posixpath
import warnings

from airflow import settings
from airflow.hooks.hdfs_hook import HdfsHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class HdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS
    """

    template_fields = ('file_path',)
    ui_color = settings.WEB_COLORS['LIGHTBLUE']

    @apply_defaults
    def __init__(self,
                 file_path,
                 hdfs_conn_id='hdfs_default',
                 file_size=None,
                 ignored_ext=('_COPYING_', ),
                 ignore_copying=None,
                 hook=HdfsHook,
                 *args,
                 **kwargs):

        if ignore_copying is not None:
            warnings.warn(
                'The ignore_copying argument is no longer used and will '
                'be removed in the next version of Airflow.',
                category=PendingDeprecationWarning)

        super(HdfsSensor, self).__init__(*args, **kwargs)
        self.hdfs_conn_id = hdfs_conn_id
        self.file_path = file_path

        self.file_size = file_size
        self.ignored_ext = set(ignored_ext)

        self.hook = hook

    @property
    def filepath(self):
        warnings.warn(
            'The `filepath` property has been renamed to `file_path`. '
            'Support for the old accessor will be dropped in the next '
            'version of Airflow.',
            category=PendingDeprecationWarning)
        return self.file_path

    def poke(self, context):
        self.log.info('Poking for file %s', self.file_path)
        hdfs_conn = self.hook(self.hdfs_conn_id).get_conn()

        try:
            file_paths = hdfs_conn.glob(self.file_path)
        except IOError:
            # File path doesn't exist yet.
            return False

        self.log.info('Files matching pattern: %s', file_paths)

        if self.file_size:
            file_paths = self._filter_for_size(
                hdfs_conn, file_paths, min_size=self.file_size)
            self.log.info('Files after filtering for size: %s', file_paths)

        if self.ignored_ext:
            file_paths = self._filter_with_ext(
                file_paths, exts=self.ignored_ext)
            self.log.info('Files after filtering for extensions: %s',
                          file_paths)

        return self._not_empty(file_paths)

    @staticmethod
    def _not_empty(iterable):
        try:
            next(iterable)
            return True
        except StopIteration:
            return False

    @staticmethod
    def _filter_for_size(hdfs_conn, file_paths, min_size):
        """Filters file paths for a minimum file size."""

        min_size_mb = min_size * settings.MEGABYTE

        for file_path in file_paths:
            info = hdfs_conn.info(file_path)
            if info['kind'] == 'file' and info['size'] > min_size_mb:
                yield file_path

    @staticmethod
    def _filter_with_ext(file_paths, exts):
        """Filters any files with the given extensions."""

        for file_path in file_paths:
            # Get file extension without preceding '.'.
            file_ext = posixpath.splitext(file_path)[1][1:]

            if file_ext not in exts:
                yield file_path
