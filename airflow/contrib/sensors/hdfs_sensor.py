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

from airflow.sensors.hdfs_sensor import HdfsSensor


class HdfsSensorRegex(HdfsSensor):
    """HdfsSensor subclass that filters using a specific regex."""

    def __init__(self, file_pattern, regex, *args, **kwargs):
        if not self._is_pattern(file_pattern):
            # If file path is not a pattern, we assume it is a directory
            # containing files that we want to match the regex against.
            # This matches the legacy behaviour of the sensor.
            file_pattern = posixpath.join(file_pattern, '*')

        def _filter_regex(_, file_path):
            file_name = posixpath.basename(file_path)
            return regex.match(file_name) is not None

        super(HdfsSensorRegex, self).__init__(
            *args,
            file_pattern=file_pattern,
            extra_filters=[_filter_regex],
            **kwargs)

    @staticmethod
    def _is_pattern(path_):
        """Checks if given path contains any glob patterns."""
        return '*' in path_ or '[' in path_


class HdfsSensorFolder(HdfsSensor):
    """HdfsSensor subclass that filters specifically for directories."""

    def __init__(self,
                 be_empty=False,
                 *args,
                 **kwargs):
        super(HdfsSensorFolder, self).__init__(*args, **kwargs)
        self.be_empty = be_empty

    def poke(self, context):
        """
        poke for a non empty directory

        :return: Bool depending on the search criteria
        """
        sb = self.hook(self.hdfs_conn_id).get_conn()
        result = [f for f in sb.ls([self.filepath], include_toplevel=True)]
        result = self.filter_for_ignored_ext(result, self.ignored_ext,
                                             self.ignore_copying)
        result = self.filter_for_filesize(result, self.file_size)
        if self.be_empty:
            self.log.info('Poking for filepath {self.filepath} to a empty directory'
                          .format(**locals()))
            return len(result) == 1 and result[0]['path'] == self.filepath
        else:
            self.log.info('Poking for filepath {self.filepath} to a non empty directory'
                          .format(**locals()))
            result.pop(0)
            return bool(result) and result[0]['file_type'] == 'f'
