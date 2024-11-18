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
import struct
from typing import TYPE_CHECKING

import uuid6
from sqlalchemy import BigInteger, Column, ForeignKey, String, Text, select
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import relationship
from sqlalchemy.sql.expression import literal
from sqlalchemy_utils import UUIDType

from airflow.configuration import conf
from airflow.exceptions import DagCodeNotFound
from airflow.models.base import Base
from airflow.utils import timezone
from airflow.utils.file import open_maybe_zipped
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

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
    fileloc_hash = Column(BigInteger, nullable=False)
    fileloc = Column(String(2000), nullable=False)
    # The max length of fileloc exceeds the limit of indexing.
    created_at = Column(UtcDateTime, nullable=False, default=timezone.utcnow)
    source_code = Column(Text().with_variant(MEDIUMTEXT(), "mysql"), nullable=False)
    dag_version_id = Column(
        UUIDType(binary=False), ForeignKey("dag_version.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    dag_version = relationship("DagVersion", back_populates="dag_code", uselist=False)

    def __init__(self, dag_version, full_filepath: str, source_code: str | None = None):
        self.dag_version = dag_version
        self.fileloc = full_filepath
        self.fileloc_hash = DagCode.dag_fileloc_hash(self.fileloc)
        self.last_updated = timezone.utcnow()
        self.source_code = source_code or DagCode.code(self.fileloc)

    @classmethod
    @provide_session
    def write_code(cls, dag_version: DagVersion, fileloc: str, session: Session = NEW_SESSION) -> DagCode:
        """
        Write code into database.

        :param fileloc: file path of DAG to sync
        :param session: ORM Session
        """
        log.debug("Writing DAG file %s into DagCode table", fileloc)
        dag_code = DagCode(dag_version, fileloc, cls._get_code_from_file(fileloc))
        session.add(dag_code)
        log.debug("DAG file %s written into DagCode table", fileloc)
        return dag_code

    @classmethod
    @provide_session
    def has_dag(cls, fileloc: str, session: Session = NEW_SESSION) -> bool:
        """
        Check a file exist in dag_code table.

        :param fileloc: the file to check
        :param session: ORM Session
        """
        fileloc_hash = cls.dag_fileloc_hash(fileloc)
        return (
            session.scalars(
                select(literal(True)).where(cls.fileloc_hash == fileloc_hash).limit(1)
            ).one_or_none()
            is not None
        )

    @classmethod
    def get_code_by_fileloc(cls, fileloc: str) -> str:
        """
        Return source code for a given fileloc.

        :param fileloc: file path of a DAG
        :return: source code as string
        """
        return cls.code(fileloc)

    @classmethod
    @provide_session
    def code(cls, fileloc, session: Session = NEW_SESSION) -> str:
        """
        Return source code for this DagCode object.

        :return: source code as string
        """
        return cls._get_code_from_db(fileloc, session)

    @staticmethod
    def _get_code_from_file(fileloc):
        try:
            with open_maybe_zipped(fileloc, "r") as f:
                code = f.read()
            return code
        except FileNotFoundError:
            test_mode = conf.get("core", "unit_test_mode")
            if test_mode:
                return "source_code"
            raise

    @classmethod
    @provide_session
    def _get_code_from_db(cls, fileloc, session: Session = NEW_SESSION) -> str:
        dag_code = session.scalar(
            select(cls)
            .where(cls.fileloc_hash == cls.dag_fileloc_hash(fileloc))
            .order_by(cls.created_at.desc())
            .limit(1)
        )
        if not dag_code:
            raise DagCodeNotFound()
        else:
            code = dag_code.source_code
        return code

    @staticmethod
    def dag_fileloc_hash(full_filepath: str) -> int:
        """
        Hashing file location for indexing.

        :param full_filepath: full filepath of DAG file
        :return: hashed full_filepath
        """
        # Hashing is needed because the length of fileloc is 2000 as an Airflow convention,
        # which is over the limit of indexing.
        import hashlib

        # Only 7 bytes because MySQL BigInteger can hold only 8 bytes (signed).
        return (
            struct.unpack(
                ">Q", hashlib.sha1(full_filepath.encode("utf-8"), usedforsecurity=False).digest()[-8:]
            )[0]
            >> 8
        )
