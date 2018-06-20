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
#

from builtins import super

import hdfs3
from hdfs3.utils import MyNone

from .base import FsHook


class Hdfs3Hook(FsHook):
    """Hook for interacting with files over HDFS."""

    def __init__(self, conn_id=None):
        super().__init__(conn_id=conn_id)
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            if self._conn_id is None:
                self._conn = hdfs3.HDFileSystem()
            else:
                config = self.get_connection(self._conn_id)
                config_extra = config.extra_dejson

                # Extract hadoop parameters from extra.
                pars = config_extra.get('pars', {})

                # Collect extra parameters to pass to kwargs.
                extra_kws = {}
                if config.login:
                    extra_kws['user'] = config.login

                # Build connection.
                self._conn = hdfs3.HDFileSystem(
                    host=config.host or MyNone,
                    port=config.port or MyNone,
                    pars=pars,
                    **extra_kws)

        return self._conn

    def disconnect(self):
        if self._conn is not None:
            self._conn.disconnect()
        self._conn = None

    def open(self, file_path, mode='rb'):
        return self.get_conn().open(file_path, mode=mode)

    def isdir(self, path):
        return self.get_conn().isdir(path)

    def exists(self, file_path):
        return self.get_conn().exists(file_path)

    def makedir(self, dir_path, mode=0e755, exist_ok=True):
        conn = self.get_conn()

        if conn.exists(dir_path):
            if not exist_ok:
                self._raise_dir_exists(dir_path)
        else:
            conn.mkdir(dir_path)
            conn.chmod(dir_path, mode=mode)

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if not exist_ok and self.exists(dir_path):
            self._raise_dir_exists(dir_path)
        self.get_conn().makedirs(dir_path, mode=mode)

    def walk(self, dir_path):
        for tup in self.get_conn().walk(dir_path):
            yield tup

    def rm(self, file_path):
        self.get_conn().rm(file_path, recursive=False)

    def rmtree(self, dir_path):
        self.get_conn().rm(dir_path, recursive=True)
