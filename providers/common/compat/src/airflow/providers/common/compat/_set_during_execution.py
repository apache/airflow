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
"""
Airflow 2 stand-in for the Task SDK's ``SET_DURING_EXECUTION`` sentinel.

A decorated operator passes the sentinel for an argument its callable fills in, such as the prompt
of ``@task.llm``. Airflow 2 stores a template field it cannot JSON-encode as ``str(value)``, which
for a bare ``NOTSET`` is an object address: it differs in every process, so the serialized Dag's
hash and the rendered template view change with it. This sentinel renders the way Airflow 3's
does. :mod:`airflow.providers.common.compat.sdk` tries the SDK first, so on Airflow 3 this module
is never imported.
"""

from __future__ import annotations

from airflow.utils.types import ArgNotSet  # type: ignore[attr-defined]  # Airflow 2 only


class SetDuringExecution(ArgNotSet):
    """Sentinel for an argument that is set during execution, not at parse time."""

    def __repr__(self) -> str:
        return "DYNAMIC (set during execution)"


SET_DURING_EXECUTION = SetDuringExecution()
