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

from airflow import settings
from airflow.hooks.hdfs_hook import HdfsHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class HdfsFileSensor(BaseSensorOperator):
    """Sensor that waits for files matching a specific (glob) pattern to land in HDFS.

    :param str file_pattern: Glob pattern to match.
    :param str conn_id: Connection to use.
    :param Iterable[FilePathFilter] filters: Optional list of filters that can be
        used to apply further filtering to any file paths matching the glob pattern.
        Any files that fail a filter are dropped from consideration.
    :param int min_size: Minimum size (in MB) for files to be considered. Can be used
        to filter any intermediate files that are below the expected file size.
    :param Set[str] ignore_exts: File extensions to ignore. By default, files with
        a '_COPYING_' extension are ignored, as these represent temporary files.
    """

    template_fields = ("_pattern",)
    ui_color = settings.WEB_COLORS["LIGHTBLUE"]

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
        filters = list(filters or [])

        if min_size:
            filters.append(SizeFilter(min_size=min_size))

        if ignore_exts:
            filters.append(ExtFilter(exts=ignore_exts))

        self._pattern = pattern
        self._conn_id = conn_id
        self._filters = filters

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
            for filter_ in self._filters:
                file_paths = filter_(file_paths, hook)
            file_paths = list(file_paths)

            self.log.info("Filters after filtering: %s", file_paths)

            return bool(file_paths)


class HdfsFolderSensor(BaseSensorOperator):
    """Waits for folder(s) matching the given pattern to be created in HDFS.

    By default, the sensor does not check whether any matched folders contain
    files. If folders are required to (not) be empty, this behaviour can be modified
    using the `require_empty` and `require_not_empty` parameters. If `require_empty`
    is True, the sensor fails if any matched folders contain files. Similarly, if
    `require_not_empty` is True, the sensor fails if any matched folders do not contain
    files. The list of file paths considered in these checks can be modified using the
    `sub_pattern` and `sub_filters` parameters.

    :param str pattern: Glob pattern to match.
    :param str conn_id: Connection to use.
    :param bool require_empty: Whether folders are required to be empty. If true,
        the sensor fails if any of the matched directories is not empty.
    :param bool require_not_empty: Whether folders are required to be NOT empty.
        If true, the sensor fails if any of the matched directories is empty.
    :param str sub_pattern: Glob pattern to filter nested file paths on when
        checking whether directories are (not) empty.
    :param Iterable[FilePathFilter] sub_filters: File path filters that should be used
        to filter nested file paths on when checking whether directories are (not)
        empty. See `HdfsFileSensor` for more details.
    """

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
        self._sub_filters = list(sub_filters or [])

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
                    "Checking for files or subdirectories matching pattern: %s",
                    self._sub_pattern,
                )

                # Check if directories do/don't contain files. Returns False
                # if any of the directories fail the required condition.
                for dir_path in dir_paths:
                    sub_pattern = posixpath.join(dir_path, self._sub_pattern)
                    sub_paths = conn.glob(sub_pattern)

                    for filter_ in self._sub_filters:
                        sub_paths = filter_(sub_paths, hook)

                    sub_paths = list(sub_paths)

                    self.log.info(
                        "Sub-directories/files matching pattern: %s", sub_paths
                    )

                    is_empty = not sub_paths
                    if (self._require_empty and not is_empty) or (
                        self._require_not_empty and is_empty
                    ):
                        return False

            return bool(dir_paths)


class FilePathFilter:
    """Base file path filter class.

    Allows a given list of file paths to be filtered on a given criteria.
    Examples include a size filter (which filters files for a given minimum file
    size) and a extension filter (which filters files with a given extension).

    Filters are required to implement a single __call__ method, which takes the
    file system hook and a set of file paths to filter. As a result, this method
    should return the set of filtered file paths.
    """

    def __call__(self, file_paths, hook):
        raise NotImplementedError()


class SizeFilter(FilePathFilter):
    """Filter that drops any file paths below the given file size.

    :param int min_size: Minimum file size in megabytes.
    """

    def __init__(self, min_size):
        self._min_size = min_size

    def __call__(self, file_paths, hook):
        min_size_mb = self._min_size * settings.MEGABYTE

        conn = hook.get_conn()
        for file_path in file_paths:
            info = conn.info(file_path)

            if info["kind"] == "file" and info["size"] > min_size_mb:
                yield file_path


class ExtFilter(FilePathFilter):
    """Filter that drops any file paths with given extensions.

    :param Set[str] exts: Set of extensions to filter for.
    """

    def __init__(self, exts):
        self._exts = set(exts)

    def __call__(self, file_paths, hook):
        for file_path in file_paths:
            if posixpath.splitext(file_path)[1][1:] not in self._exts:
                yield file_path
