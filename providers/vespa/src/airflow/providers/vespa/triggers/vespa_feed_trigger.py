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

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent


class VespaFeedTrigger(BaseTrigger):
    """Trigger that runs a Vespa feed, update, or delete operation."""

    def __init__(
        self,
        docs: list[dict[str, Any]],
        conn_info: dict[str, Any],
        operation_type: str = "feed",
        feed_kwargs: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self.docs = list(docs)
        self.conn_info = conn_info
        self.operation_type = operation_type
        self.feed_kwargs = feed_kwargs or {}

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize VespaFeedTrigger arguments and class path."""
        return (
            "airflow.providers.vespa.triggers.vespa_feed_trigger.VespaFeedTrigger",
            {
                "docs": self.docs,
                "conn_info": self.conn_info,
                "operation_type": self.operation_type,
                "feed_kwargs": self.feed_kwargs,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Create the hook and execute the feed operation in the default executor."""
        try:
            # Lazy import: avoids pulling in pyvespa (and its transitive deps)
            # at triggerer parse time.
            from airflow.providers.vespa.hooks.vespa import VespaHook

            hook = VespaHook.from_resolved_connection(
                host=self.conn_info["host"],
                port=self.conn_info.get("port"),
                schema=self.conn_info.get("schema"),
                namespace=self.conn_info.get("namespace"),
                extra=self.conn_info.get("extra", {}),
            )

            # pyvespa's feed_async_iterable is synchronous/blocking, so we
            # offload it to a thread to keep the triggerer event loop responsive.
            loop = asyncio.get_running_loop()

            def _feed() -> dict[str, Any]:
                return hook.feed_iterable(
                    self.docs,
                    operation_type=self.operation_type,
                    **self.feed_kwargs,
                )

            summary = await loop.run_in_executor(None, _feed)

            if summary["errors"]:
                self.log.error(
                    "Vespa feed operation failed: %d error(s). Details: %s",
                    summary["errors"],
                    summary["error_details"],
                )
                yield TriggerEvent(
                    {
                        "success": False,
                        "sent": summary["sent"],
                        "errors": summary["error_details"],
                    }
                )
            else:
                self.log.info(
                    "Vespa feed operation completed successfully for %d document(s)",
                    len(self.docs),
                )
                yield TriggerEvent({"success": True, "sent": summary["sent"]})
        except Exception as err:
            error_msg = f"Trigger failed: {type(err).__name__}: {err}"
            self.log.exception(error_msg)
            yield TriggerEvent({"success": False, "errors": [{"error": error_msg}]})
