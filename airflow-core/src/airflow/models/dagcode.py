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
from typing import TYPE_CHECKING

import uuid6
from sqlalchemy import Column, ForeignKey, String, Text, select
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import relationship
from sqlalchemy.sql.expression import literal
from sqlalchemy_utils import UUIDType

from airflow.configuration import conf
from airflow.exceptions import DagCodeNotFound
from airflow.models.base import ID_LEN, Base
from airflow.utils import timezone
from airflow.utils.file import open_maybe_zipped
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.models.dag_version import DagVersion

log = logging.getLogger(__name__)


class DagCode(Base):
    """
    A table for DAGs code.

    dag_code table contains code of DAG files synchronized by scheduler.

    For details on dag serialization see SerializedDagModel
    """

    __tablename__ = "dag_code"
    id = Column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)
    dag_id = Column(String(ID_LEN), nullable=False)
    fileloc = Column(String(2000), nullable=False)
    # The max length of fileloc exceeds the limit of indexing.
    created_at = Column(UtcDateTime, nullable=False, default=timezone.utcnow)
    last_updated = Column(UtcDateTime, nullable=False, default=timezone.utcnow, onupdate=timezone.utcnow)
    source_code = Column(Text().with_variant(MEDIUMTEXT(), "mysql"), nullable=False)
    source_code_hash = Column(String(32), nullable=False)
    dag_version_id = Column(
        UUIDType(binary=False), ForeignKey("dag_version.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    dag_version = relationship("DagVersion", back_populates="dag_code", uselist=False)

    def __init__(self, dag_version, full_filepath: str, source_code: str | None = None):
        self.dag_version = dag_version
        self.fileloc = full_filepath
        self.source_code = source_code or DagCode.code(self.dag_version.dag_id)
        self.source_code_hash = self.dag_source_hash(self.source_code)
        self.dag_id = dag_version.dag_id

    @classmethod
    @provide_session
    def write_code(cls, dag_version: DagVersion, fileloc: str, session: Session = NEW_SESSION) -> DagCode:
        """
        Write code into database.

        :param fileloc: file path of DAG to sync
        :param session: ORM Session
        """
        log.debug("Writing DAG file %s into DagCode table", fileloc)
        dag_code = DagCode(dag_version, fileloc, cls.get_code_from_file(fileloc))
        session.add(dag_code)
        log.debug("DAG file %s written into DagCode table", fileloc)
        return dag_code

    @classmethod
    @provide_session
    def has_dag(cls, dag_id: str, session: Session = NEW_SESSION) -> bool:
        """
        Check a dag exists in dag code table.

        :param dag_id: the dag_id of the DAG
        :param session: ORM Session
        """
        return (
            session.scalars(select(literal(True)).where(cls.dag_id == dag_id).limit(1)).one_or_none()
            is not None
        )

    @classmethod
    @provide_session
    def code(cls, dag_id, session: Session = NEW_SESSION) -> str:
        """
        Return source code for this DagCode object.

        :return: source code as string
        """
        return cls._get_code_from_db(dag_id, session)

    @staticmethod
    def get_code_from_file(fileloc):
        try:
            with open_maybe_zipped(fileloc, "r") as f:
                code = f.read()
            return code
        except FileNotFoundError:
            test_mode = conf.getboolean("core", "unit_test_mode")
            if test_mode:
                return "source_code"
            raise

    @classmethod
    @provide_session
    def _get_code_from_db(cls, dag_id, session: Session = NEW_SESSION) -> str:
        dag_code = session.scalar(
            select(cls).where(cls.dag_id == dag_id).order_by(cls.last_updated.desc()).limit(1)
        )
        if not dag_code:
            raise DagCodeNotFound()
        code = dag_code.source_code
        return code

    @staticmethod
    def dag_source_hash(source: str) -> str:
        """
        Hash the source code of the DAG.

        This is needed so we can update the source on code changes
        """
        return md5(source.encode("utf-8")).hexdigest()

    @classmethod
    def _latest_dagcode_select(cls, dag_id: str) -> Select:
        """
        Get the select object to get the latest dagcode.

        :param dag_id: The DAG ID.
        :return: The select object.
        """
        return select(cls).where(cls.dag_id == dag_id).order_by(cls.last_updated.desc()).limit(1)

    @classmethod
    @provide_session
    def get_latest_dagcode(cls, dag_id: str, session: Session = NEW_SESSION) -> DagCode | None:
        """
        Get the latest dagcode.

        :param dag_id: The DAG ID.
        :param session: The database session.
        :return: The latest dagcode or None if not found.
        """
        return session.scalar(cls._latest_dagcode_select(dag_id))

    @classmethod
    @provide_session
    def update_source_code(cls, dag_id: str, fileloc: str, session: Session = NEW_SESSION) -> None:
        """
        Check if the source code of the DAG has changed and update it if needed.

        :param dag_id: Dag ID
        :param fileloc: The path of code file to read the code from
        :param session: The database session.
        :return: None
        """
        latest_dagcode = cls.get_latest_dagcode(dag_id, session)
        if not latest_dagcode:
            return
        new_source_code = cls.get_code_from_file(fileloc)
        new_source_code_hash = cls.dag_source_hash(new_source_code)
        if new_source_code_hash != latest_dagcode.source_code_hash:
            latest_dagcode.source_code = new_source_code
            latest_dagcode.source_code_hash = new_source_code_hash
            session.merge(latest_dagcode)
