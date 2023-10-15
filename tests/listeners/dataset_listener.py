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

import copy
import typing

from airflow.listeners import hookimpl

if typing.TYPE_CHECKING:
    from airflow.datasets import Dataset


changed: list[Dataset] = []
created: list[Dataset] = []


@hookimpl
def on_dataset_changed(dataset):
    changed.append(copy.deepcopy(dataset))


@hookimpl
def on_dataset_created(dataset):
    created.append(copy.deepcopy(dataset))


def clear():
    global changed, created
    changed, created = [], []
