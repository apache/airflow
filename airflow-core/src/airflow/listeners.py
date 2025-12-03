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
"""Compatibility imports for listeners module.

This module provides backwards compatibility for imports from airflow.listeners.
The actual implementation is now in airflow._shared.listeners.
"""

from __future__ import annotations

# Re-export all listener functionality for backwards compatibility
from airflow._shared.listeners import *  # noqa: F403, F401
from airflow._shared.listeners import hookimpl  # noqa: F401
from airflow._shared.listeners.listener import ListenerManager, get_listener_manager  # noqa: F401

# Re-export spec modules for backwards compatibility
from airflow._shared.listeners import spec  # noqa: F401

__all__ = [
    "hookimpl",
    "ListenerManager",
    "get_listener_manager",
    "spec",
]
