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


def deprecated(new_name=None):
    """This is a decorator which can be used to mark functions
       as deprecated. It will result in a warning being emitted
       when the function is used.
    """

    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            if new_name:
                message = ("{} has been deprecated and will be replaced by "
                           "{} in a future version of Airflow."
                           .format(function.__name__, new_name))
            else:
                message = ("{} has been deprecated and will be removed "
                           "in a future version of Airflow."
                           .format(function.__name__))
            warnings.warn(message, category=DeprecationWarning)
            return function(*args, **kwargs)
        return wrapper
    return decorator


class HdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS.
    """

    template_fields = ('file_path',)
    ui_color = settings.WEB_COLORS['LIGHTBLUE']

    @deprecated_args(renamed={'filepath': 'file_pattern',
                              'file_size': 'min_size',
                              'ignored_ext': 'ignore_exts'},
                     dropped={'ignore_copying'})
    @apply_defaults
    def __init__(self,
                 file_pattern,
                 hdfs_conn_id='hdfs_default',
                 min_size=None,
                 ignore_exts=('_COPYING_', ),
                 extra_filters=None,
                 hook=HdfsHook,
                 *args,
                 **kwargs):
        super(HdfsSensor, self).__init__(*args, **kwargs)

        default_filters = self._default_filters(
            min_size=min_size, ignore_exts=ignore_exts)
        filters = default_filters + (extra_filters or [])

        self._file_pattern = file_pattern
        self._conn_id = hdfs_conn_id

        self._min_size = min_size
        self._ignore_exts = set(ignore_exts)

        self._hook = hook(hdfs_conn_id)
        self._filters = filters

    @property
    def file_pattern(self):
        """File pattern (glob) that the sensor matches against."""
        return self._file_pattern

    @property
    def conn_id(self):
        """Connection ID used by the sensor."""
        return self._conn_id

    @deprecated(new_name="file_pattern")
    @property
    def filepath(self):
        return self.file_pattern

    @deprecated(new_name="conn_id")
    @property
    def hdfs_conn_id(self):
        return self.conn_id

    @deprecated()
    @property
    def min_size(self):
        return self._min_size

    @deprecated()
    @property
    def ignored_ext(self):
        return self._ignore_exts

    @classmethod
    def _default_filters(cls, min_size=None, ignore_exts=None):
        filters = []

        if min_size is not None:
            def _size_filter(hook, file_path):
                return cls._filter_size(hook, file_path, min_size=min_size)
            filters.append(_size_filter)

        if ignore_exts:
            def _ext_filter(hook, file_path):
                return cls._filter_ext(hook, file_path, exts=ignore_exts)
            filters.append(_ext_filter)

        return filters

    @staticmethod
    def _filter_size(hook, file_path, min_size):
        """Filters any files below a minimum file size."""

        info = hook.get_conn().info(file_path)
        min_size_mb = min_size * settings.MEGABYTE

        return info['kind'] == 'file' and info['size'] > min_size_mb

    @staticmethod
    def _filter_ext(_, file_path, exts):
        """Filters any files with the given extensions."""

        file_ext = posixpath.splitext(file_path)[1][1:]
        return file_ext not in exts

    def poke(self, context):
        self.log.info('Poking for file pattern %s', self.file_pattern)
        hdfs_conn = self._hook.get_conn()

        try:
            file_paths = hdfs_conn.glob(self.file_pattern)
        except IOError:
            # File path doesn't exist yet.
            return False

        self.log.info('Files matching pattern: %s', file_paths)

        file_paths = self._apply_filters(
            file_paths, self._filters, hook=self._hook)
        self.log.info('Filters after filtering: %s', file_paths)

        return self._not_empty(file_paths)

    @staticmethod
    def _apply_filters(file_paths, filter_funcs, hook):
        """Filters file paths that fail any of the filters (i.e. any
           of the filter functions returns true for a given file).
        """

        for file_path in file_paths:
            for func in filter_funcs:
                if not func(hook, file_path):
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
