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

from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat._compat_utils import create_module_getattr
from airflow.providers.common.compat.version_compat import (
    AIRFLOW_V_3_1_PLUS,
    AIRFLOW_V_3_2_PLUS,
    AIRFLOW_V_3_3_PLUS,
)

_IMPORT_MAP: dict[str, str | tuple[str, ...]] = {
    # Re-export from sdk (which handles Airflow 2.x/3.x fallbacks)
    "BaseBranchOperator": "airflow.providers.common.compat.sdk",
    "BaseOperator": "airflow.providers.common.compat.sdk",
    "BaseAsyncOperator": "airflow.providers.common.compat.sdk",
    "BranchMixIn": "airflow.providers.common.compat.sdk",
    "get_current_context": "airflow.providers.common.compat.sdk",
    "is_async_callable": "airflow.providers.common.compat.sdk",
    # Standard provider items with direct fallbacks
    "PythonOperator": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "ShortCircuitOperator": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "_SERIALIZERS": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
}

if TYPE_CHECKING:
    from airflow.sdk.bases.decorator import is_async_callable
    from airflow.sdk.bases.operator import BaseAsyncOperator
elif AIRFLOW_V_3_2_PLUS:
    from airflow.sdk.bases.decorator import is_async_callable
    from airflow.sdk.bases.operator import BaseAsyncOperator
else:
    if AIRFLOW_V_3_1_PLUS:
        from airflow.sdk import BaseOperator
    else:
        from airflow.models import BaseOperator

    def is_async_callable(func) -> bool:
        """Detect if a callable is an async function."""
        import inspect
        from functools import partial

        while isinstance(func, partial):
            func = func.func
        return inspect.iscoroutinefunction(func)

    class BaseAsyncOperator(BaseOperator):
        """Stub for Airflow < 3.2 that raises a clear error."""

        @property
        def is_async(self) -> bool:
            return True

        async def aexecute(self, context):
            raise NotImplementedError()

        def execute(self, context):
            raise RuntimeError(
                "Async operators require Airflow 3.2+. Upgrade Airflow or use a synchronous callable."
            )


if TYPE_CHECKING:
    from airflow.sdk import ResumableJobMixin
elif AIRFLOW_V_3_3_PLUS:
    from airflow.sdk import ResumableJobMixin
else:

    class ResumableJobMixin:  # type: ignore[no-redef]
        """
        Stub for Airflow < 3.3, where ``ResumableJobMixin`` does not exist yet.

        It lets an operator keep ``ResumableJobMixin`` in its bases on every supported Airflow
        version; the durable, crash-safe path the real mixin drives is gated to 3.3+ at the call
        site, so on older Airflow the operator runs its non-durable path unchanged.
        """

        external_id_key: str = "remote_job_id"

        def __init__(self, *, durable: bool = True, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.durable = durable

        def execute_resumable(self, context):
            external_id = self.submit_job(context)
            self.poll_until_complete(external_id, context)
            return self.get_job_result(external_id, context)


__getattr__ = create_module_getattr(import_map=_IMPORT_MAP)

__all__ = sorted([*_IMPORT_MAP.keys(), "ResumableJobMixin"])
