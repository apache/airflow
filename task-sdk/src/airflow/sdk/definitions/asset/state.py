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

from airflow.sdk.execution_time.context import AssetStateAccessor


class AssetState(AssetStateAccessor):
    """
    Access the state store for a single asset from anywhere a SUPERVISOR_COMMS
    channel is available (task, callback, or trigger).

    This is the equivalent of subscripting ``context['asset_state'][asset]``
    inside a task, but usable from contexts where ``context`` is not bound -
    most notably from inside a :class:`BaseEventTrigger`.

    Identify the asset by either ``name`` or ``uri`` (exactly one is required)::

        from airflow.sdk import AssetState

        asset_state = AssetState(name="my_asset")
        watermark = asset_state.get("watermark")
        asset_state.set("watermark", "2026-01-01")
    """

    def __init__(self, *, name: str | None = None, uri: str | None = None) -> None:
        super().__init__(name=name, uri=uri)
