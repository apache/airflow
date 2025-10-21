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

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKeyConstraint, Index, String, Text, delete, select, true
from sqlalchemy.orm import Mapped, relationship

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.models.dag import DagModel
from airflow.utils.retries import retry_db_transaction
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, mapped_column

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class DagWarning(Base):
    """
    A table to store DAG warnings.

    DAG warnings are problems that don't rise to the level of failing the DAG parse
    but which users should nonetheless be warned about. These warnings are recorded
    when parsing DAG and displayed on the Webserver in a flash message.
    """

    dag_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    warning_type: Mapped[str] = mapped_column(String(50), primary_key=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, default=timezone.utcnow)

    dag_model = relationship("DagModel", viewonly=True, lazy="selectin")

    __tablename__ = "dag_warning"
    __table_args__ = (
        ForeignKeyConstraint(
            ("dag_id",),
            ["dag.dag_id"],
            name="dcw_dag_id_fkey",
            ondelete="CASCADE",
        ),
        Index("idx_dag_warning_dag_id", dag_id),
    )

    def __init__(self, dag_id: str, warning_type: str, message: str, **kwargs):
        super().__init__(**kwargs)
        self.dag_id = dag_id
        self.warning_type = DagWarningType(warning_type).value  # make sure valid type
        self.message = message

    def __eq__(self, other) -> bool:
        return self.dag_id == other.dag_id and self.warning_type == other.warning_type

    def __hash__(self) -> int:
        return hash((self.dag_id, self.warning_type))

    @classmethod
    @provide_session
    @retry_db_transaction
    def purge_inactive_dag_warnings(cls, session: Session = NEW_SESSION) -> None:
        """
        Deactivate DagWarning records for inactive dags.

        :return: None
        """
        if session.get_bind().dialect.name == "sqlite":
            dag_ids_stmt = select(DagModel.dag_id).where(DagModel.is_stale == true())
            query = delete(cls).where(cls.dag_id.in_(dag_ids_stmt.scalar_subquery()))
        else:
            query = delete(cls).where(cls.dag_id == DagModel.dag_id, DagModel.is_stale == true())

        session.execute(query.execution_options(synchronize_session=False))
        session.commit()


class DagWarningType(str, Enum):
    """
    Enum for DAG warning types.

    This is the set of allowable values for the ``warning_type`` field
    in the DagWarning model.
    """

    ASSET_CONFLICT = "asset conflict"
    NONEXISTENT_POOL = "non-existent pool"
