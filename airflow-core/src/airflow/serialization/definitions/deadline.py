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

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.models.callback import CallbackDefinitionProtocol


@dataclass
class SerializedAssetDeadline:
    """Serialized representation of an AssetDeadline."""

    callback: CallbackDefinitionProtocol
    cron: str | None = None
    interval: timedelta | Any = None
    on_resolve: CallbackDefinitionProtocol | None = None
    name: str | None = None

    @property
    def reference_dict(self) -> dict[str, Any]:
        if self.cron is not None:
            return {"reference_type": "AssetScheduleDeadline", "cron": self.cron}
        return {"reference_type": "AssetLastEventDeadline"}
