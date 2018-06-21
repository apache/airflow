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

from builtins import open, str
import os
import shutil

from .base import FsHook


class LocalFsHook(FsHook):
    """Hook for interacting with local files on the local file system."""

    sep = os.sep

    def get_conn(self):
        return None

    def open(self, file_path, mode='rb'):
        return open(str(file_path), mode=mode)

    def exists(self, file_path):
        return os.path.exists(str(file_path))

    def isdir(self, path):
        return os.path.isdir(path)

    def makedir(self, dir_path, mode=0o755, exist_ok=True):
        if os.path.exists(dir_path):
            if not exist_ok:
                self._raise_dir_exists(dir_path)
        else:
            os.mkdir(dir_path, mode)

    def makedirs(self, dir_path, mode=0o755, exist_ok=True):
        if os.path.exists(dir_path):
            if not exist_ok:
                self._raise_dir_exists(dir_path)
        else:
            os.makedirs(str(dir_path), mode=mode)

    def listdir(self, dir_path):
        return os.listdir(dir_path)

    def walk(self, dir_path):
        for tup in os.walk(dir_path):
            yield tup

    def rm(self, file_path):
        os.unlink(str(file_path))

    def rmtree(self, dir_path):
        shutil.rmtree(str(dir_path))

    @staticmethod
    def join(path, *paths):
        return os.path.join(path, *paths)

    @staticmethod
    def split(path):
        return os.path.split(path)
