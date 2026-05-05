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
"""Core OpenLineage primitives: runtime-state probe and raw-event emission."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airflow.providers.openlineage import conf
from airflow.providers.openlineage.plugins.listener import get_openlineage_listener

if TYPE_CHECKING:
    from openlineage.client.event_v2 import RunEvent

log = logging.getLogger(__name__)

__all__ = ["emit", "is_openlineage_active"]


def is_openlineage_active() -> bool:
    """
    Return ``True`` when OpenLineage is enabled and its listener is available.

    Useful to short-circuit work that only makes sense when OpenLineage is going to
    actually emit events (for example, fetching extra metadata from a warehouse solely
    to attach it to lineage facets).
    """
    if conf.is_disabled():
        log.debug("OpenLineage is disabled.")
        return False
    if get_openlineage_listener() is None:
        log.debug("OpenLineage listener is not available.")
        return False
    return True


def emit(event: RunEvent) -> None:
    """Emit a pre-built OpenLineage event through the provider's listener."""
    if not is_openlineage_active():
        log.info("OpenLineage is not active - emit will have no effect.")
        return
    get_openlineage_listener().adapter.emit(event)
