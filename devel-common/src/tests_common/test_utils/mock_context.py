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

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any
from unittest import mock

from tests_common.test_utils.compat import Context
from tests_common.test_utils.taskinstance import create_task_instance

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def mock_context(task) -> Context:
    from airflow.models import TaskInstance
    from airflow.utils.session import NEW_SESSION

    from tests_common.test_utils.compat import XCOM_RETURN_KEY

    values: dict[str, Any] = {}

    class MockedTaskInstance(TaskInstance):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.values: dict[str, Any] = {}

        def xcom_pull(
            self,
            task_ids: str | Iterable[str] | None = None,
            dag_id: str | None = None,
            key: str = XCOM_RETURN_KEY,
            include_prior_dates: bool = False,
            session: Session = NEW_SESSION,
            *,
            map_indexes: int | Iterable[int] | None = None,
            default: Any = None,
            run_id: str | None = None,
        ) -> Any:
            key = f"{self.task_id}_{self.dag_id}_{key}"
            if map_indexes is not None and (not isinstance(map_indexes, int) or map_indexes >= 0):
                key += f"_{map_indexes}"
            return values.get(key, default)

        def xcom_push(self, key: str, value: Any, session: Session = NEW_SESSION, **kwargs) -> None:
            key = f"{self.task_id}_{self.dag_id}_{key}"
            if self.map_index is not None and self.map_index >= 0:
                key += f"_{self.map_index}"
            values[key] = value

    values["ti"] = create_task_instance(task, dag_version_id=mock.MagicMock(), ti_type=MockedTaskInstance)

    # See https://github.com/python/mypy/issues/8890 - mypy does not support passing typed dict to TypedDict
    return Context(values)  # type: ignore[misc]
