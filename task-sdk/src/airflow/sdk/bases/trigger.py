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
from typing import Any


@dataclass
class StartTriggerArgs:
    """Arguments required for start task execution from triggerer."""

    trigger_cls: str
    next_method: str
    trigger_kwargs: dict[str, Any] | None = None
    next_kwargs: dict[str, Any] | None = None
    timeout: timedelta | None = None
