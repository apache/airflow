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

import re
import sys
from builtins import str

from airflow import settings
from airflow.hooks.hdfs_hook import HDFSHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.log.logging_mixin import LoggingMixin


class HdfsSensor(BaseSensorOperator):
    """
    Waits for a file or folder to land in HDFS
    """
    template_fields = ('filepath',)
    ui_color = settings.WEB_COLORS['LIGHTBLUE']

    @apply_defaults
    def __init__(self,
                 filepath,
                 hdfs_conn_id='hdfs_default',
                 ignored_ext=None,
                 ignore_copying=True,
                 file_size=None,
                 hook=HDFSHook,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if ignored_ext is None:
            ignored_ext = ['_COPYING_']
        self.filepath = filepath
        self.hdfs_conn_id = hdfs_conn_id
        self.file_size = file_size
        self.ignored_ext = ignored_ext
        self.ignore_copying = ignore_copying
        self.hook = hook

    @staticmethod
    def filter_for_filesize(result, size=None):
        """
        Will test the filepath result and test if its size is at least self.filesize

        :param result: a list of dicts returned by Snakebite ls
        :param size: the file size in MB a file should be at least to trigger True
        :return: (bool) depending on the matching criteria
        """
        if size:
            log = LoggingMixin().log
            log.debug(
                'Filtering for file size >= %s in files: %s',
                size, map(lambda x: x['path'], result)
            )
            size *= settings.MEGABYTE
            result = [x for x in result if x['length'] >= size]
            log.debug('HdfsSensor.poke: after size filter result is %s', result)
        return result

    @staticmethod
    def filter_for_ignored_ext(result, ignored_ext, ignore_copying):
        """
        Will filter if instructed to do so the result to remove matching criteria

        :param result: list of dicts returned by Snakebite ls
        :type result: list[dict]
        :param ignored_ext: list of ignored extensions
        :type ignored_ext: list
        :param ignore_copying: shall we ignore ?
        :type ignore_copying: bool
        :return: list of dicts which were not removed
        :rtype: list[dict]
        """
        if ignore_copying:
            log = LoggingMixin().log
            regex_builder = r"^.*\.(%s$)$" % '$|'.join(ignored_ext)
            ignored_extensions_regex = re.compile(regex_builder)
            log.debug(
                'Filtering result for ignored extensions: %s in files %s',
                ignored_extensions_regex.pattern, map(lambda x: x['path'], result)
            )
            result = [x for x in result if not ignored_extensions_regex.match(x['path'])]
            log.debug('HdfsSensor.poke: after ext filter result is %s', result)
        return result

    def poke(self, context):
        sb = self.hook(self.hdfs_conn_id).get_conn()
        self.log.info('Poking for file %s', self.filepath)
        try:
            # IMOO it's not right here, as there no raise of any kind.
            # if the filepath is let's say '/data/mydirectory',
            # it's correct but if it is '/data/mydirectory/*',
            # it's not correct as the directory exists and sb does not raise any error
            # here is a quick fix
            result = [f for f in sb.ls([self.filepath], include_toplevel=False)]
            self.log.debug('HdfsSensor.poke: result is %s', result)
            result = self.filter_for_ignored_ext(
                result, self.ignored_ext, self.ignore_copying
            )
            result = self.filter_for_filesize(result, self.file_size)
            return bool(result)
        except Exception:
            e = sys.exc_info()
            self.log.debug("Caught an exception !: %s", str(e))
            return False
