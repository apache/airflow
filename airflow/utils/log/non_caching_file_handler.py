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
from __future__ import annotations

import os
from logging import FileHandler
from logging.handlers import RotatingFileHandler
from typing import IO


def make_file_io_non_caching(io: IO[str]) -> IO[str]:
    try:
        fd = io.fileno()
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    except Exception:
        # in case either file descriptor cannot be retrieved or fadvise is not available
        # we should simply return the wrapper retrieved by FileHandler's open method
        # the advice to the kernel is just an advice and if we cannot give it, we won't
        pass
    return io


class NonCachingFileHandler(FileHandler):
    """
    This is an extension of the python FileHandler that advises the Kernel to not cache the file
    in PageCache when it is written. While there is nothing wrong with such cache (it will be cleaned
    when memory is needed), it causes ever-growing memory usage when scheduler is running as it keeps
    on writing new log files and the files are not rotated later on. This might lead to confusion
    for our users, who are monitoring memory usage of Scheduler - without realising that it is
    harmless and expected in this case.

    See https://github.com/apache/airflow/issues/14924

    Adding the advice to Kernel might help with not generating the cache memory growth in the first place.
    """

    def _open(self):
        return make_file_io_non_caching(super()._open())


class NonCachingRotatingFileHandler(RotatingFileHandler):
    """
    This is an extension of the python RotatingFileHandler that advises the Kernel to not cache the file
    in PageCache when it is written. While there is nothing wrong with such cache (it will be cleaned
    when memory is needed), it causes ever-growing memory usage when scheduler is running as it keeps
    on writing new log files and the files are not rotated later on. This might lead to confusion
    for our users, who are monitoring memory usage of Scheduler - without realising that it is
    harmless and expected in this case.

    See https://github.com/apache/airflow/issues/27065

    Adding the advice to Kernel might help with not generating the cache memory growth in the first place.
    """

    def _open(self):
        return make_file_io_non_caching(super()._open())
