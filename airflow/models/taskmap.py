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

"""Table to store information about mapped task instances (AIP-42)."""

import collections.abc
import enum
from typing import TYPE_CHECKING, Any, Iterator

from sqlalchemy import CheckConstraint, Column, ForeignKeyConstraint, Index, Integer
from sqlalchemy.orm import relationship

from airflow.configuration import conf
from airflow.exceptions import UnmappableXComLengthPushed, UnmappableXComTypePushed, XComForMappingNotPushed
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import ExtendedJSON

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


class TaskMapVariant(enum.Enum):
    """Task map variant.

    Possible values are **dict** (for a key-value mapping) and **list** (for an
    ordered value sequence).
    """

    DICT = "dict"
    LIST = "list"


class TaskMap(Base):
    """Model to track dynamic task-mapping information.

    This is currently only populated by an upstream TaskInstance pushing an
    XCom that's pulled by a downstream for mapping purposes.
    """

    __tablename__ = "task_map"

    # Link to upstream TaskInstance creating this dynamic mapping information.
    dag_run_id = Column(Integer(), nullable=False, primary_key=True)
    task_id = Column(StringID(), nullable=False, primary_key=True)
    map_index = Column(Integer(), nullable=False, primary_key=True)

    # If the upstream XCom is used for expand_kwargs(), we use this to store
    # lengths of each item in the dict, i.e. {"a": [1, 2]} generates a TaskMap
    # with item set to "a". This is an empty string for normal expand() calls.
    item = Column(StringID(), nullable=False, primary_key=True)

    # Denormalized for easier lookup.
    dag_id = Column(StringID(), nullable=False)
    run_id = Column(StringID(), nullable=False)

    length = Column(Integer, nullable=False)
    keys = Column(ExtendedJSON, nullable=True)  # Set if the value is a dict.

    __table_args__ = (
        CheckConstraint(length >= 0, name="task_map_length_not_negative"),
        # Ideally we should create a unique index over (dag_id, task_id, run_id, item),
        # but it goes over MySQL's index length limit. So we instead index 'item'
        # separately, and enforce uniqueness with DagRun.id instead.
        Index("idx_task_map_item", item),
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_map_task_instance_fkey",
            ondelete="CASCADE",
        ),
    )

    dag_run = relationship(
        "DagRun",
        primaryjoin="TaskMap.dag_run_id == foreign(DagRun.id)",
        uselist=False,
        lazy="joined",
        passive_deletes="all",
    )

    @classmethod
    def from_task_instance_xcom(cls, ti: "TaskInstance", value: Any, *, item: str) -> "TaskMap":
        assert ti.run_id is not None, "cannot record task map for unrun task instance"
        if value is None:
            raise XComForMappingNotPushed()
        if not isinstance(value, collections.abc.Collection) or isinstance(value, (bytes, str)):
            raise UnmappableXComTypePushed(value)
        max_map_length = conf.getint("core", "max_map_length", fallback=1024)
        if len(value) > max_map_length:
            raise UnmappableXComLengthPushed(value, max_map_length)
        return cls(
            dag_run_id=ti.dag_run.id,
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index,
            item=item,
            length=len(value),
            keys=(list(value) if isinstance(value, collections.abc.Mapping) else None),
        )

    @classmethod
    def from_unpacking_task_instance_xcom(cls, ti: "TaskInstance", coll: dict) -> Iterator["TaskMap"]:
        for key, val in coll.items():
            if not isinstance(key, str):
                raise ValueError(f"cannot unpack {coll} as keyword arguments")
            yield cls.from_task_instance_xcom(ti, val, item=key)

    @property
    def variant(self) -> TaskMapVariant:
        if self.keys is None:
            return TaskMapVariant.LIST
        return TaskMapVariant.DICT
