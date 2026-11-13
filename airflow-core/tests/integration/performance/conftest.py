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

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest

PERF_DIR = Path(__file__).resolve().parent


def pytest_ignore_collect(config: pytest.Config) -> bool | None:
    """Collect this folder only when it is named on the pytest command line."""
    # CI's integration job collects everything under tests/integration, the parent, and runs each
    # testable integration in its own cell; without this gate the long run here would start there.
    # Naming the folder, a file in it or a test id from it is the switch; nothing else selects it.
    invocation_dir = Path(config.invocation_params.dir)
    for arg in config.args:
        target = Path(str(arg).split("::", 1)[0])
        if not target.is_absolute():
            target = invocation_dir / target
        target = target.resolve()
        if target == PERF_DIR or PERF_DIR in target.parents:
            return None
    return True
