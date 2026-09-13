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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.api_fastapi.common.db.assets import resolve_triggering_event_refs
from airflow.api_fastapi.core_api.datamodels.assets import AssetEventResponse

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.asset import AssetEvent


def serialize_asset_events(events: Sequence[AssetEvent], *, session: Session) -> list[AssetEventResponse]:
    """
    Serialize asset events, flagging on each created dag run whether this event triggered it.

    Only a run's most recent consumed event triggers it; the rest were merely included. The flag is
    resolved per ``(event_id, dag_id, run_id)`` tuple and set on the run before validation so the response
    carries it.
    """
    triggering_refs = resolve_triggering_event_refs(events, session=session)
    asset_events = []
    for event in events:
        for run in event.created_dagruns:
            run.triggering = (event.id, run.dag_id, run.run_id) in triggering_refs
        asset_events.append(AssetEventResponse.model_validate(event))
    return asset_events
