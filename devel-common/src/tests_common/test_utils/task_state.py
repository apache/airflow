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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from unittest import mock

RESERVED_KEY_PREFIX = "__airflow_"


def get_non_reserved_task_state_writes(task_state_store: mock.Mock) -> list[mock._Call]:
    """
    Return the ``set`` calls an operator made on a mocked task state store.

    From Airflow 3.5 (Task SDK 1.5) the durable execution mixin also writes ``__airflow_`` keys for
    the logs banner, and older Airflow versions do not, so tests that pin the operator's own writes
    skip those keys.
    """
    return [
        call
        for call in task_state_store.set.call_args_list
        if not str(call.args[0]).startswith(RESERVED_KEY_PREFIX)
    ]


def drop_reserved_task_state_keys(values: dict) -> dict:
    """Return a fake task state store's contents without the reserved ``__airflow_`` keys."""
    return {key: value for key, value in values.items() if not str(key).startswith(RESERVED_KEY_PREFIX)}
