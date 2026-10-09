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
"""An in-memory durable journal storage, for tests that simulate task attempts."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from airflow.providers.common.ai.durable.base import build_step_key


class MemoryStorage:
    """
    A ``DurableStorageProtocol`` kept in a dict, shared by the attempts of one test.

    Entries go through a JSON round-trip on the way in, as they do in both real backends,
    so a test sees what a retry would read back rather than the live objects.
    """

    def __init__(self) -> None:
        self.entries: dict[str, dict[str, Any]] = {}
        self.refuse: set[str] = set()

    def save_step(self, key: str, entry: dict[str, Any]) -> bool:
        if key in self.refuse:
            return False
        self.entries[key] = json.loads(json.dumps(entry))
        return True

    def load_step(self, key: str) -> dict[str, Any] | None:
        entry = self.entries.get(key)
        return json.loads(json.dumps(entry)) if entry is not None else None

    def delete_steps(self, keys: Iterable[str]) -> None:
        for key in keys:
            self.entries.pop(key, None)

    def steps(self, run: int = 0) -> list[dict[str, Any]]:
        """Return the entries of one run in position order."""
        prefix = build_step_key(run, 0).removesuffix("0")
        positions = sorted(int(key.removeprefix(prefix)) for key in self.entries if key.startswith(prefix))
        return [self.entries[build_step_key(run, position)] for position in positions]
