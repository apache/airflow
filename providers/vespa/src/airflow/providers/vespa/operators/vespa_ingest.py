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

import json
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowException, BaseHook, BaseOperator, TaskDeferred
from airflow.providers.vespa.hooks.vespa import VALID_OPERATION_TYPES, VespaHook
from airflow.providers.vespa.triggers.vespa_feed_trigger import VespaFeedTrigger

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class VespaIngestOperator(BaseOperator):
    """Defer a Vespa feed, update, or delete operation to a trigger."""

    template_fields: Sequence[str] = ("docs", "vespa_conn_id")

    def __init__(
        self,
        *,
        docs: Iterable[dict[str, Any]],
        vespa_conn_id: str = "vespa_default",
        operation_type: str = "feed",
        feed_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.docs = docs
        self.vespa_conn_id = vespa_conn_id
        if operation_type not in VALID_OPERATION_TYPES:
            raise ValueError(
                f"Invalid operation_type {operation_type!r}. Must be one of {sorted(VALID_OPERATION_TYPES)}"
            )
        self.operation_type = operation_type
        self.feed_kwargs = feed_kwargs or {}

    def execute(self, context: Context) -> None:
        """Resolve the Vespa connection in the worker and defer ingestion to a trigger."""
        conn = BaseHook.get_connection(self.vespa_conn_id)
        extra = conn.extra_dejson or {}

        self.docs = list(self.docs) if not isinstance(self.docs, list) else self.docs

        for i, doc in enumerate(self.docs):
            if not isinstance(doc, dict):
                raise TypeError(f"docs[{i}] must be a dict, got {type(doc).__name__}")

        # feed_kwargs are serialized into the TriggerEvent; catch bad values early.
        if self.feed_kwargs:
            try:
                json.dumps(self.feed_kwargs)
            except (TypeError, ValueError) as err:
                raise ValueError(
                    f"feed_kwargs must be JSON-serializable for trigger deferral: {err}"
                ) from err

        # Resolve the connection here (worker process) because trigger processes
        # run inside the triggerer and cannot access the Airflow metadata DB.
        conn_info = {
            "host": conn.host,
            "port": conn.port,
            "schema": conn.schema,
            "namespace": VespaHook._get_field(extra, "namespace") or "default",
            "extra": extra,
        }

        raise TaskDeferred(
            trigger=VespaFeedTrigger(
                docs=self.docs,
                conn_info=conn_info,
                operation_type=self.operation_type,
                feed_kwargs=self.feed_kwargs,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> dict[str, int]:
        """Return a compact success payload or raise on trigger failure."""
        if not event["success"]:
            raise AirflowException(
                f"{len(event['errors'])} document(s) failed; Error details: {event['errors']}"
            )
        return {"ingested": event.get("sent", 0)}
