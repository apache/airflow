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
"""Triggers of the system test Dags, in a module of their own so the triggerer can import them."""

from __future__ import annotations

from typing import Any

from airflow.triggers.base import BaseTrigger


class FailingTrigger(BaseTrigger):
    """Fail in the triggerer, which makes the deferred task resume only to fail."""

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return f"{type(self).__module__}.{type(self).__qualname__}", {}

    async def run(self):
        raise ValueError("Intentional failure of the trigger")
        yield  # Makes it an async generator, as the triggerer expects
