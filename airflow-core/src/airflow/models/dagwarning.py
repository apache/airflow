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
from typing import Annotated

from pydantic import StringConstraints, TypeAdapter
from sqlalchemy import ForeignKeyConstraint, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime

WARNING_TYPE_MAX_LENGTH = 50


class DagWarning(Base):
    """
    A table to store DAG warnings.

    DAG warnings are problems that don't rise to the level of failing the DAG parse
    but which users should nonetheless be warned about. These warnings are recorded
    when parsing DAG and displayed on the Webserver in a flash message.
    """

    dag_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    warning_type: Mapped[str] = mapped_column(String(WARNING_TYPE_MAX_LENGTH), primary_key=True)
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
        self.warning_type = get_warning_type_value(warning_type)
        self.message = message

    def __eq__(self, other) -> bool:
        return self.dag_id == other.dag_id and self.warning_type == other.warning_type

    def __hash__(self) -> int:
        return hash((self.dag_id, self.warning_type))


class DagWarningType(str, Enum):
    """
    Enum for DAG warning types.

    This is the set of allowable values for the ``warning_type`` field
    in the DagWarning model.
    """

    ASSET_CONFLICT = "asset conflict"
    DUPLICATE_DAG_ID = "duplicate dag id"
    NONEXISTENT_POOL = "non-existent pool"
    RUNTIME_VARYING_VALUE = "runtime varying value"
    TASK_GROUP_CYCLE = "task group cycle"


ImporterWarningType = Annotated[
    str,
    StringConstraints(pattern=r"^[a-z][a-z0-9_]*:[a-z0-9_.\-]+$", max_length=WARNING_TYPE_MAX_LENGTH),
]
"""Warning type reported by a Dag importer, prefixed with its namespace (e.g. ``yaml:deprecated_field``)."""

DagWarningTypeValue = DagWarningType | ImporterWarningType
"""Any valid ``warning_type``: a built-in :class:`DagWarningType` or an :data:`ImporterWarningType`."""

_warning_type_adapter: TypeAdapter[DagWarningTypeValue] = TypeAdapter(DagWarningTypeValue)


def get_warning_type_value(warning_type: str) -> str:
    """
    Return the value to store for ``warning_type``.

    :raises ValueError: if it is neither a :class:`DagWarningType` nor an :data:`ImporterWarningType`.
    """
    validated = _warning_type_adapter.validate_python(warning_type)
    return validated.value if isinstance(validated, DagWarningType) else validated
