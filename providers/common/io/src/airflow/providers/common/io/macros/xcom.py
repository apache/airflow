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

from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import XCOM_RETURN_KEY
from airflow.providers.common.io.xcom.backend import XComObjectStorageBackend

if TYPE_CHECKING:
    from airflow.sdk.types import RuntimeTaskInstanceProtocol


def xcom_path(
    ti: RuntimeTaskInstanceProtocol,
    task_id: str,
    key: str = XCOM_RETURN_KEY,
    *,
    dag_id: str | None = None,
    run_id: str | None = None,
    map_index: int | None = None,
    protocol: bool = True,
) -> str:
    """
    Return where an XCom value is stored in object storage, for a template to hand to another tool.

    A query engine that reads files from object storage can then read a large XCom value itself, instead of
    the value being loaded into the task and passed on::

        SELECT * FROM read_json('{{ macros.common_io.xcom_path(ti, "extract") }}')

    :param ti: The task instance rendering the template (``ti`` in the template context).
    :param task_id: The task id of the task instance that pushed the XCom.
    :param key: The key of the XCom.
    :param dag_id: The Dag id of the task instance that pushed it; the rendering task's by default.
    :param run_id: The Dag run id of the task instance that pushed it; the rendering task's by default.
    :param map_index: The map index of the task instance that pushed it; *None* (default) for a non-mapped
        task.
    :param protocol: Whether to return the full URL (``s3://bucket/...``, the default) or only the path
        (``bucket/...``, for a tool given the file system separately).
    :raises TypeError: if the configured XCom backend is not ``XComObjectStorageBackend``.
    :raises ValueError: if the XCom has no path: it does not exist, or its value is stored in the database
        (smaller than ``[common.io] xcom_objectstorage_threshold``). A *None* rendered into a query would
        fail later with a less helpful error.
    """
    path = XComObjectStorageBackend.get_path(
        key=key,
        dag_id=dag_id or ti.dag_id,
        task_id=task_id,
        run_id=run_id or ti.run_id,
        map_index=map_index,
    )
    if path is None:
        raise ValueError(
            f"The XCom {key!r} from task {task_id!r} has no path: it does not exist, or it is stored in the "
            "database (smaller than [common.io] xcom_objectstorage_threshold)"
        )
    return str(path) if protocol else path.path
