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

import logging
import random
import string
from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Integer, select
from sqlalchemy.orm import relationship

from airflow.models.base import Base, StringID
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dagcode import DagCode
    from airflow.models.serialized_dag import SerializedDagModel

log = logging.getLogger(__name__)


class DagVersion(Base):
    """Model to track the versions of DAGs in the database."""

    __tablename__ = "dag_version"
    id = Column(Integer, primary_key=True)
    version_number = Column(Integer)
    version_name = Column(StringID())
    dag_id = Column(StringID(), ForeignKey("dag.dag_id", ondelete="CASCADE"))
    dag_model = relationship("DagModel", back_populates="dag_versions")
    dag_code = relationship("DagCode", back_populates="dag_version", uselist=False)
    serialized_dag = relationship("SerializedDagModel", back_populates="dag_version", uselist=False)
    dag_runs = relationship("DagRun", back_populates="dag_version")
    task_instances = relationship("TaskInstance", back_populates="dag_version")

    def __init__(
        self,
        *,
        dag_id: str,
        version_number: int,
        dag_code: DagCode,
        serialized_dag: SerializedDagModel,
        version_name: str | None = None,
    ):
        self.dag_id = dag_id
        self.version_number = version_number
        self.dag_code = dag_code
        self.serialized_dag = serialized_dag
        self.version_name = version_name

    def __repr__(self):
        return f"<DagVersion {self.dag_id} - {self.version_name}>"

    @classmethod
    def _generate_random_string(cls):
        letters = string.ascii_letters + string.digits
        return "dag-" + "".join(random.choice(letters) for i in range(10))

    @classmethod
    @provide_session
    def _generate_unique_random_string(cls, session: Session = NEW_SESSION):
        while True:
            random_str = cls._generate_random_string()
            # Check if the generated string exists
            if not session.scalar(select(cls).where(cls.version_name == random_str)):
                return random_str

    @classmethod
    @provide_session
    def write_dag(
        cls,
        *,
        dag_id: str,
        dag_code: DagCode,
        serialized_dag: SerializedDagModel,
        version_name: str | None = None,
        session: Session = NEW_SESSION,
    ):
        """Write a new DagVersion into database."""
        existing_dag_version = session.scalar(
            select(cls).where(cls.dag_id == dag_id).order_by(cls.version_number.desc()).limit(1)
        )
        version_number = 1

        if existing_dag_version:
            version_number = existing_dag_version.version_number + 1
        if not version_name and existing_dag_version:
            version_name = existing_dag_version.version_name

        dag_version = DagVersion(
            dag_id=dag_id,
            version_number=version_number,
            dag_code=dag_code,
            serialized_dag=serialized_dag,
            version_name=version_name or cls._generate_unique_random_string(session),
        )
        log.debug("Writing DagVersion %s to the DB", dag_version)
        session.add(dag_version)
        log.debug("DagVersion %s written to the DB", dag_version)
        return dag_version

    @classmethod
    @provide_session
    def get_latest_version(cls, dag_id: str, session: Session = NEW_SESSION):
        return session.scalar(
            select(cls).where(cls.dag_id == dag_id).order_by(cls.version_number.desc()).limit(1)
        )

    @classmethod
    @provide_session
    def get_version(
        cls,
        dag_id: str,
        version_name: str | None = None,
        version_number: int | None = None,
        session: Session = NEW_SESSION,
    ):
        version_select_obj = select(cls).where(cls.dag_id == dag_id)
        if version_name:
            version_select_obj = version_select_obj.where(cls.version_name == version_name)
        if version_number:
            version_select_obj = version_select_obj.where(cls.version_number == version_number)
        version_select_obj = version_select_obj.order_by(cls.version_number.desc()).limit(1)
        return session.scalar(version_select_obj)

    @property
    def version(self):
        if not self.version_name and not self.version_number:
            return None
        sep = "-"
        return self.version_name + sep + str(self.version_number)
