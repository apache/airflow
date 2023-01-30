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
from __future__ import annotations

import logging

from airflow.cli.commands.task_command import TaskCommandMarker
from airflow.listeners import hookimpl

log = logging.getLogger(__name__)


class FileWriteListener:
    def __init__(self, path):
        self.path = path

    def write(self, line: str):
        with open(self.path, "a") as f:
            f.write(line + "\n")

    @hookimpl
    def on_starting(self, component):
        if isinstance(component, TaskCommandMarker):
            self.write("on_starting")

    @hookimpl
    def before_stopping(self, component):
        if isinstance(component, TaskCommandMarker):
            self.write("before_stopping")
