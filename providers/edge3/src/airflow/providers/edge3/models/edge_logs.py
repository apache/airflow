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

from sqlalchemy import (
    Integer,
    Text,
    text,
)
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import Mapped

from airflow.models.base import Base, StringID
from airflow.providers.common.compat.sqlalchemy.orm import mapped_column
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.sqlalchemy import UtcDateTime


class EdgeLogsModel(Base, LoggingMixin):
    """
    Temporary collected logs from a Edge Worker while job runs on remote site.

    As the Edge Worker in most cases has a local file system and the web UI no access
    to read files from remote site, Edge Workers will send incremental chunks of logs
    of running jobs to the central site. As log storage backends in most cloud cases can not
    append logs, the table is used as buffer to receive. Upon task completion logs can be
    flushed to task log handler.

    Log data therefore is collected in chunks and is only temporary.
    """

    __tablename__ = "edge_logs"
    dag_id: Mapped[str] = mapped_column(StringID(), primary_key=True, nullable=False)
    task_id: Mapped[str] = mapped_column(StringID(), primary_key=True, nullable=False)
    run_id: Mapped[str] = mapped_column(StringID(), primary_key=True, nullable=False)
    map_index: Mapped[int] = mapped_column(
        Integer, primary_key=True, nullable=False, server_default=text("-1")
    )
    try_number: Mapped[int] = mapped_column(Integer, primary_key=True, default=0)
    log_chunk_time: Mapped[datetime] = mapped_column(UtcDateTime, primary_key=True, nullable=False)
    log_chunk_data: Mapped[str] = mapped_column(Text().with_variant(MEDIUMTEXT(), "mysql"), nullable=False)

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
