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

import functools
import posixpath
import warnings

from airflow import settings
from airflow.hooks.hdfs_hook import HdfsHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


def deprecated_args(renamed=None, dropped=None):
    """Decorator for the deprecation of renamed/removed keyword arguments.

    Wraps functions with changed keyword arguments (either renamed to new
    arguments or dropped entirely). Functions calls with deprecated arguments
    raise appropriate warnings. For removed arguments, any given values are
    ignored (outside of the warning). For renamed arguments, values are
    transparently proxied to their new argument names.
    """

    def decorator(function):

        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            new_kwargs = {}

            for key in kwargs:
                if key in dropped:
                    warnings.warn(
                        'Argument {!r} is no longer supported and will be'
                        'removed in a future version of Airflow.'.format(key),
                        category=DeprecationWarning)
                elif key in renamed:
                    warnings.warn(
                        'Argument {!r} has been renamed to {!r}. The old name '
                        'will no longer be supported in a future version of '
                        'Airflow.'.format(key, renamed[key]),
                        category=DeprecationWarning)

                    new_kwargs[renamed[key]] = kwargs[key]
                else:
                    new_kwargs[key] = kwargs[key]

            return function(*args, **new_kwargs)
        return wrapper
    return decorator


class HdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS.
    """

    template_fields = ('file_path',)
    ui_color = settings.WEB_COLORS['LIGHTBLUE']

    @deprecated_args(renamed={'filepath': 'file_path',
                              'file_size': 'min_size'},
                     dropped={'ignore_copying'})
    @apply_defaults
    def __init__(self,
                 file_path=None,
                 hdfs_conn_id='hdfs_default',
                 filters=None,
                 hook=HdfsHook,
                 min_size=None,
                 ignored_ext=('_COPYING_', ),
                 *args,
                 **kwargs):

        if filters is None:
            filters = []

        filters = self._setup_filters(
            filters, min_size=min_size, ignored_exts=ignored_ext)

        super(HdfsSensor, self).__init__(*args, **kwargs)
        self.hdfs_conn_id = hdfs_conn_id
        self.file_path = file_path

        self.min_size = min_size
        self.ignored_ext = set(ignored_ext)

        self.hook = hook
        self._filters = filters

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

        file_paths = self._apply_filters(file_paths, self._filters, hdfs_conn)
        self.log.info('Filters after filtering: %s', file_paths)

        return self._not_empty(file_paths)

    @staticmethod
    def _apply_filters(file_paths, filter_funcs, conn):
        """Filters file paths that fail any of the filters (i.e. any
           of the filter functions returns true for a given file).
        """

        for file_path in file_paths:
            for func in filter_funcs:
                if not func(conn, file_path):
                    break
            else:
                yield file_path

    @staticmethod
    def _not_empty(iterable):
        """Returns true if iterable is not empty."""

        try:
            next(iterable)
            return True
        except StopIteration:
            return False

    @classmethod
    def _setup_filters(cls, filters, min_size, ignored_exts):
        """Returns default filters."""

        filters = list(filters)

        if min_size is not None:
            filters.append(
                lambda conn, fp: cls._filter_size(
                    conn, fp, min_size=min_size))

        if ignored_exts:
            filters.append(lambda conn, fp: cls._filter_ext(fp, ignored_exts))

        return filters

    @staticmethod
    def _filter_size(hdfs_conn, file_path, min_size):
        """Filters any files below a minimum file size."""

        info = hdfs_conn.info(file_path)
        min_size_mb = min_size * settings.MEGABYTE

        return info['kind'] == 'file' and info['size'] > min_size_mb

    @staticmethod
    def _filter_ext(file_path, exts):
        """Filters any files with the given extensions."""

        file_ext = posixpath.splitext(file_path)[1][1:]
        return file_ext not in exts
