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

from functools import cache

from airflow.plugins_manager import integrate_listener_plugins
from airflow.sdk._shared.listeners.listener import (
    ListenerManager,
    get_listener_manager as _create_listener_manager,
)


@cache
def get_listener_manager() -> ListenerManager:
    """Get singleton listener manager."""
    _listener_manager = _create_listener_manager()
    integrate_listener_plugins(_listener_manager)
    return _listener_manager


__all__ = ["get_listener_manager", "ListenerManager"]
