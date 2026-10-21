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
Stand-in for ``airflow.sdk.ResumableJobMixin`` on Airflow versions below 3.3.

``ResumableJobMixin`` and the ``task_state_store`` it relies on arrived in Airflow 3.3. An operator
that imports the mixin from :mod:`airflow.providers.common.compat.sdk` keeps working on older
versions: this stand-in submits a new job on every try, exactly what the operator did before it
became durable. That module tries the SDK first, so on 3.3 and later it hands out the SDK class and
this one is never imported.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context

# Tells "durable left at its default" apart from an explicit durable=True/False, so only an explicit
# value warns that it has no effect here.
_DURABLE_UNSET: Any = object()


class ResumableJobMixin:
    """Airflow < 3.3 stand-in: no ``task_state_store`` to resume from, so every try submits a new job."""

    external_id_key: str = "remote_job_id"

    def __init__(self, *, durable: bool = _DURABLE_UNSET, **kwargs: Any) -> None:
        if durable is not _DURABLE_UNSET:
            warnings.warn(
                "`durable` has no effect on Airflow versions below 3.3.",
                UserWarning,
                stacklevel=2,
            )
        super().__init__(**kwargs)
        self.durable = False

    def execute_resumable(self, context: Context) -> Any:
        external_id = self.submit_job(context)  # type: ignore[attr-defined]
        self.poll_until_complete(external_id, context)  # type: ignore[attr-defined]
        return self.get_job_result(external_id, context)  # type: ignore[attr-defined]
