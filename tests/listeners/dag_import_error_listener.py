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

from airflow.listeners import hookimpl

new = {}
existing = {}


@hookimpl
def on_new_dag_import_error(filename, stacktrace):
    """Execute when new dag import error appears"""
    new["filename"] = stacktrace
    print("new error>> filename:" + str(filename))
    print("new error>> stacktrace:" + str(stacktrace))


@hookimpl
def on_existing_dag_import_error(filename, stacktrace):
    """Execute when existing dag import error appears"""
    existing["filename"] = stacktrace
    print("existing error>> filename:" + str(filename))
    print("existing error>> stacktrace:" + str(stacktrace))


def clear():
    global new, existing
    new, existing = {}, {}
