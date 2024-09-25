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

from sqlalchemy import Boolean, Column, Index, String

from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.utils.sqlalchemy import UtcDateTime


class ToggleDag(Base):
    """
    Model that stores the recent state of the DAG pause/unpause and sends notification if state changes
    It is used to keep track of state of DAG over time and to avoid double triggering alert emails.
    """

    __tablename__ = "toggle_dag"

    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    last_verified_time = Column(UtcDateTime)
    is_dag_paused = Column(Boolean)

    __table_args__ = (Index("toggle_dag", dag_id, unique=False),)

    def __repr__(self):
        return str((self.dag_id, self.last_verified_time.isoformat()))
