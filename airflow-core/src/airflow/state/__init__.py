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

import threading

from airflow._shared.state import (
    AssetScope as AssetScope,
    BaseStateBackend as BaseStateBackend,
    StateScope as StateScope,
    TaskScope as TaskScope,
)


def resolve_state_backend() -> type[BaseStateBackend]:
    from airflow.configuration import conf

    clazz = conf.getimport("state_store", "backend")
    if clazz is None:
        raise ValueError("state_store.backend is not configured or resolved to None.")
    if not issubclass(clazz, BaseStateBackend):
        raise TypeError(
            f"Your custom state backend `{clazz.__name__}` is not a subclass of `BaseStateBackend`."
        )
    return clazz


_backend_instance: BaseStateBackend | None = None
_backend_lock = threading.Lock()


def get_state_backend() -> BaseStateBackend:
    """Return a cached instance of the configured state backend."""
    global _backend_instance
    if _backend_instance is None:
        with _backend_lock:
            if _backend_instance is None:
                _backend_instance = resolve_state_backend()()
    return _backend_instance
