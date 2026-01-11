#
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

from typing import Any, Protocol

from structlog.typing import FilteringBoundLogger

__all__ = [
    "Logger",
]


class Logger(FilteringBoundLogger, Protocol):  # noqa: D101
    name: str

    def isEnabledFor(self, level: int): ...
    def getEffectiveLevel(self) -> int: ...

    # FilteringBoundLogger defines these methods with `event: str` -- in a few places in Airflow we do
    # `self.log.exception(e)` or `self.log.info(rule_results_df)` so we correct the types to allow for this
    # (as the code already did)
    def debug(self, event: Any, *args: Any, **kw: Any) -> Any: ...
    def info(self, event: Any, *args: Any, **kw: Any) -> Any: ...
    def warning(self, event: Any, *args: Any, **kw: Any) -> Any: ...
    def error(self, event: Any, *args: Any, **kw: Any) -> Any: ...
    def exception(self, event: Any, *args: Any, **kw: Any) -> Any: ...
    def log(self, level: int, event: Any, *args: Any, **kw: Any) -> Any: ...
