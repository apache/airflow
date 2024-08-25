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

from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    Integer,
    Text,
    text,
)
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.configuration import conf
from airflow.models.base import Base, StringID
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.serialization.serialized_objects import add_pydantic_class_type_mapping
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.pydantic import BaseModel as BaseModelPydantic, ConfigDict, is_pydantic_2_installed
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


class RemoteLogsModel(Base, LoggingMixin):
    """
    Temporary collected logs from a remote worker while job runs remote.

    As the remote worker in most cases has a local file system and the web UI no access
    to read files from remote, remote workers will send incremental chunks of logs
    of running jobs to the central site. As log storage backends in most cloud cases can not
    append logs, the table is used as buffer to receive. Upon task completion logs can be
    flushed to task log handler.

    Log data therefore is collected in chunks and is only temporary.
    """

    __tablename__ = "remote_logs"
    dag_id = Column(StringID(), primary_key=True, nullable=False)
    task_id = Column(StringID(), primary_key=True, nullable=False)
    run_id = Column(StringID(), primary_key=True, nullable=False)
    map_index = Column(Integer, primary_key=True, nullable=False, server_default=text("-1"))
    try_number = Column(Integer, primary_key=True, default=0)
    log_chunk_time = Column(UtcDateTime, primary_key=True, nullable=False)
    log_chunk_data = Column(Text().with_variant(MEDIUMTEXT(), "mysql"), nullable=False)

    def __init__(
        self,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int,
        try_number: int,
        log_chunk_time: datetime,
        log_chunk_data: str,
    ):
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index
        self.try_number = try_number
        self.log_chunk_time = log_chunk_time
        self.log_chunk_data = log_chunk_data
        super().__init__()


class RemoteLogs(BaseModelPydantic, LoggingMixin):
    """Accessor for remote worker instances as logical model."""

    dag_id: str
    task_id: str
    run_id: str
    map_index: int
    try_number: int
    log_chunk_time: datetime
    log_chunk_data: str
    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    @staticmethod
    @internal_api_call
    @provide_session
    def push_logs(
        task: TaskInstanceKey | tuple,
        log_chunk_time: datetime,
        log_chunk_data: str,
        session: Session = NEW_SESSION,
    ) -> None:
        """Push an incremental log chunk from remote worker to central site."""
        if isinstance(task, tuple):
            task = TaskInstanceKey(*task)
        log_chunk = RemoteLogsModel(
            dag_id=task.dag_id,
            task_id=task.task_id,
            run_id=task.run_id,
            map_index=task.map_index,
            try_number=task.try_number,
            log_chunk_time=log_chunk_time,
            log_chunk_data=log_chunk_data,
        )
        session.add(log_chunk)
        # Write logs to local file to make them accessible
        logfile_path = RemoteLogs.logfile_path(task)
        if not logfile_path.exists():
            new_folder_permissions = int(
                conf.get("logging", "file_task_handler_new_folder_permissions", fallback="0o775"), 8
            )
            logfile_path.parent.mkdir(parents=True, exist_ok=True, mode=new_folder_permissions)
        with logfile_path.open("a") as logfile:
            logfile.write(log_chunk_data)

    @staticmethod
    @lru_cache
    def logfile_path(task: TaskInstanceKey) -> Path:
        """Elaborate the path and filename to expect from task execution."""
        from airflow.utils.log.file_task_handler import FileTaskHandler

        ti = TaskInstance.get_task_instance(
            dag_id=task.dag_id,
            run_id=task.run_id,
            task_id=task.task_id,
            map_index=task.map_index,
        )
        if TYPE_CHECKING:
            assert ti
        base_log_folder = conf.get("logging", "base_log_folder", fallback="NOT AVAILABLE")
        return Path(base_log_folder, FileTaskHandler(base_log_folder)._render_filename(ti, task.try_number))


if is_pydantic_2_installed():
    RemoteLogs.model_rebuild()

add_pydantic_class_type_mapping("remote_worker", RemoteLogsModel, RemoteLogs)
