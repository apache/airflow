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

import multiprocessing


class SharedLogger:
    def __init__(self, str_to_count: str, level: str):
        self.counter = multiprocessing.Value('i')
        self.str_to_count = str_to_count
        self.level = level

    def info(self, message, *args):
        self._count(message, "info")

    def warning(self, message, *args):
        self._count(message, "warning")

    def debug(self, message, *args):
        self._count(message, "debug")

    def error(self, message, *args):
        self._count(message, "error")

    def _count(self, message, level):
        if level != self.level:
            return
        if message == self.str_to_count:
            self.counter.value += 1
