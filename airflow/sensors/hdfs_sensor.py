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

from airflow import settings
from airflow.hooks.hdfs_hook import HdfsHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.deprecation import deprecated_args, deprecated, RenamedClass


class HdfsFileSensor(BaseSensorOperator):
    """Waits for file(s) to land in HDFS."""

    template_fields = ("_pattern",)
    ui_color = settings.WEB_COLORS["LIGHTBLUE"]

    @deprecated_args(
        renamed={
            "filepath": "file_pattern",
            "hdfs_conn_id": "conn_id",
            "file_size": "min_size",
            "ignored_ext": "ignore_exts",
        },
        dropped={"ignore_copying", "hook"},
    )
    @apply_defaults
    def __init__(
        self,
        pattern,
        conn_id="hdfs_default",
        filters=None,
        min_size=None,
        ignore_exts=("_COPYING_",),
        **kwargs
    ):
        super(HdfsFileSensor, self).__init__(**kwargs)

        # Min-size and ignore-ext filters are added via
        # arguments for backwards compatibility.
        default_filters = self._default_filters(
            min_size=min_size, ignore_exts=ignore_exts
        )
        filters = default_filters + (filters or [])

        self._pattern = pattern
        self._conn_id = conn_id
        self._filters = filters

        self._min_size = min_size
        self._ignore_exts = set(ignore_exts)

    @property
    def pattern(self):
        """File pattern (glob) that the sensor matches against."""
        return self._pattern

    @property
    def conn_id(self):
        """ID of connection used by the sensor."""
        return self._conn_id

    # Deprecated properties that exist for backwards compatibility.

    @deprecated(new_name="file_pattern")
    @property
    def filepath(self):
        return self._pattern

    @deprecated(new_name="conn_id")
    @property
    def hdfs_conn_id(self):
        return self._conn_id

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
            filters.append(functools.partial(filter_by_size, min_size=min_size))

        if ignore_exts:
            filters.append(functools.partial(filter_for_exts, exts=ignore_exts))

        return filters

    def poke(self, context):
        with HdfsHook(self._conn_id) as hook:
            conn = hook.get_conn()

            # Fetch files matching glob pattern.
            self.log.info("Poking for file pattern %s", self._pattern)

            try:
                file_paths = [
                    fp for fp in conn.glob(self._pattern) if not conn.isdir(fp)
                ]
            except IOError:
                # File path doesn't exist yet.
                file_paths = []

            self.log.info("Files matching pattern: %s", file_paths)

            # Filter using any provided filters.
            for filter_func in self._filters:
                file_paths = filter_func(hook, file_paths)
            file_paths = list(file_paths)

            self.log.info("Filters after filtering: %s", file_paths)

            return bool(file_paths)


HdfsSensor = RenamedClass(
    "HdfsSensor",
    new_class=HdfsFileSensor,
    old_module=__name__)


class HdfsFolderSensor(BaseSensorOperator):
    """Waits for folders to lands in HDFS."""

    template_fields = ("_pattern",)
    ui_color = settings.WEB_COLORS["LIGHTBLUE"]

    def __init__(
        self,
        pattern,
        conn_id="hdfs_default",
        require_empty=False,
        require_not_empty=False,
        sub_pattern=None,
        sub_filters=None,
        **kwargs
    ):
        super(HdfsFolderSensor, self).__init__(**kwargs)

        if require_empty and require_not_empty:
            raise ValueError(
                "Either require_empty or require_not_empty must be false, "
                "as the two conditions are mutually exclusive."
            )

        self._pattern = pattern
        self._conn_id = conn_id

        self._require_empty = require_empty
        self._require_not_empty = require_not_empty

        self._sub_pattern = sub_pattern or "*"
        self._sub_filters = sub_filters or []

    def poke(self, context):
        with HdfsHook(self._conn_id) as hook:
            conn = hook.get_conn()

            # Try to expand glob pattern, returns single dir if not glob.
            self.log.info("Poking for directories matching %s", self._pattern)

            try:
                dir_paths = [
                    path_ for path_ in conn.glob(self._pattern) if conn.isdir(path_)
                ]
            except IOError:
                # File path doesn't exist yet.
                dir_paths = []

            self.log.info("Directories matching pattern: %s", dir_paths)

            if self._require_empty or self._require_not_empty:
                self.log.info(
                    "Checking for files or subdirectories " "matching pattern: %s",
                    self._sub_pattern,
                )

                # Check if directories do/don't contain files. Returns False
                # if any of the directories fail the required condition.
                for dir_path in dir_paths:
                    sub_pattern = posixpath.join(dir_path, self._sub_pattern)
                    sub_paths = conn.glob(sub_pattern)

                    for filter_func in self._sub_filters:
                        sub_paths = filter_func(hook, sub_paths)

                    sub_paths = list(sub_paths)
                    is_empty = not sub_paths

                    self.log.info(
                        "Sub-directories/files matching pattern: %s", sub_paths
                    )

                    if (self._require_empty and not is_empty) or (
                        self._require_not_empty and is_empty
                    ):
                        return False

            return bool(dir_paths)


def filter_by_size(hook, file_paths, min_size):
    """Filters any HDFS files below a minimum file size."""

    conn = hook.get_conn()
    min_size_mb = min_size * settings.MEGABYTE

    for file_path in file_paths:
        info = conn.info(file_path)

        if info["kind"] == "file" and info["size"] > min_size_mb:
            yield file_path


def filter_for_exts(_, file_paths, exts):
    """Filters any HDFS files with the given extensions."""

    for file_path in file_paths:
        if posixpath.splitext(file_path)[1][1:] not in exts:
            yield file_path
