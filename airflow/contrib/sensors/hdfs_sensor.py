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

from functools import partial
import posixpath

from airflow.sensors import hdfs_sensor
from airflow.utils.deprecation import RenamedClass, deprecated_args


class HdfsRegexFileSensor(hdfs_sensor.HdfsFileSensor):
    """HdfsSensor subclass that filters using a specific regex."""

    @deprecated_args(renamed={"filepath": "pattern"})
    def __init__(self, pattern, regex, **kwargs):
        if not self._is_pattern(pattern):
            # If file path is not a pattern, we assume it is a directory
            # containing files that we want to match the regex against.
            # This matches the legacy behaviour of the sensor.
            pattern = posixpath.join(pattern, "*")

        super(HdfsRegexFileSensor, self).__init__(
            pattern=pattern,
            filters=[partial(filter_regex, regex=regex)],
            **kwargs
        )

    @staticmethod
    def _is_pattern(path_):
        """Checks if given path contains any glob patterns."""
        return "*" in path_ or "[" in path_


def filter_regex(_, file_paths, regex):
    """Filters file paths for given regex."""

    for file_path in file_paths:
        if regex.match(posixpath.basename(file_path)):
            yield file_path


HdfsSensorRegex = RenamedClass(
    "HdfsSensorRegex", new_class=HdfsRegexFileSensor, old_module=__name__
)

HdfsSensorFolder = RenamedClass(
    "HdfsSensorFolder",
    new_class=hdfs_sensor.HdfsFolderSensor,
    old_module=__name__
)
