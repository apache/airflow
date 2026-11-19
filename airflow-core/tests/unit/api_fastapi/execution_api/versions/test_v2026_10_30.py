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

import types
from pathlib import Path

from airflow.api_fastapi.execution_api.versions import v2026_10_30


def test_module_loads_without_docstrings():
    """The module must load under ``python -OO`` / ``PYTHONOPTIMIZE=2``, where ``__doc__`` is None."""
    source = Path(v2026_10_30.__file__).read_text()
    namespace = vars(types.ModuleType(v2026_10_30.__name__))

    exec(compile(source, v2026_10_30.__file__, "exec", optimize=2), namespace)

    assert namespace["IdentifyArchivedTaskStateUpdates"].description is None
