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
"""An in-memory tag store, the two primitives an attachable backend has to provide."""

from __future__ import annotations

from collections.abc import Mapping

from airflow.providers.common.ai.sandbox.base import AttachableSandboxBackend, SandboxTerminalError


class InMemoryTagStore:
    """
    ``read_tags``/``write_tags`` over a dict keyed by sandbox handle.

    Follows the contract the base class relies on: a missing sandbox is terminal, and a
    write replaces the whole set. Mix it into a test backend beside
    ``AttachableSandboxBackend``.
    """

    def __init__(self, tags: Mapping[str, Mapping[str, str]] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tags: dict[str, dict[str, str]] = {
            sandbox: dict(values) for sandbox, values in (tags or {}).items()
        }

    def read_tags(self, sandbox: str) -> dict[str, str]:
        try:
            return dict(self.tags[sandbox])
        except KeyError:
            raise SandboxTerminalError(f"{sandbox} is gone") from None

    def write_tags(self, sandbox: str, tags: Mapping[str, str]) -> None:
        self.tags[sandbox] = dict(tags)


class TaggedBackend(InMemoryTagStore, AttachableSandboxBackend):
    """An attachable backend with an in-memory tag store and nothing else: the lifecycle raises."""

    name = "tagged"

    def create(self, *, spec=None):
        raise NotImplementedError

    def run_command(self, sandbox, command, *, timeout, max_output_bytes):
        raise NotImplementedError

    def destroy(self, sandbox):
        raise NotImplementedError
