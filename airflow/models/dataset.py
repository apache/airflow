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

from urllib.parse import urlparse

import sqlalchemy_jsonfield
from sqlalchemy import (
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Table,
    text,
)
from sqlalchemy.orm import relationship

from airflow.datasets import Dataset
from airflow.models.base import Base, StringID
from airflow.settings import json
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime


class DatasetModel(Base):
    """
    A table to store datasets.

    :param uri: a string that uniquely identifies the dataset
    :param extra: JSON field for arbitrary extra info
    """

    id = Column(Integer, primary_key=True, autoincrement=True)
    uri = Column(
        String(length=3000).with_variant(
            String(
                length=3000,
                # latin1 allows for more indexed length in mysql
                # and this field should only be ascii chars
                collation='latin1_general_cs',
            ),
            'mysql',
        ),
        nullable=False,
    )
    extra = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={})
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    consuming_dags = relationship("DagScheduleDatasetReference", back_populates="dataset")
    producing_tasks = relationship("TaskOutletDatasetReference", back_populates="dataset")

    __tablename__ = "dataset"
    __table_args__ = (
        Index('idx_uri_unique', uri, unique=True),
        {'sqlite_autoincrement': True},  # ensures PK values not reused
    )

    @classmethod
    def from_public(cls, obj: Dataset) -> DatasetModel:
        return cls(uri=obj.uri, extra=obj.extra)

    def __init__(self, uri: str, **kwargs):
        try:
            uri.encode('ascii')
        except UnicodeEncodeError:
            raise ValueError('URI must be ascii')
        parsed = urlparse(uri)
        if parsed.scheme and parsed.scheme.lower() == 'airflow':
            raise ValueError("Scheme `airflow` is reserved.")
        super().__init__(uri=uri, **kwargs)

    def __eq__(self, other):
        if isinstance(other, (self.__class__, Dataset)):
            return self.uri == other.uri
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.uri)

    def __repr__(self):
        return f"{self.__class__.__name__}(uri={self.uri!r}, extra={self.extra!r})"


class DagScheduleDatasetReference(Base):
    """References from a DAG to a dataset of which it is a consumer."""

    dataset_id = Column(Integer, primary_key=True, nullable=False)
    dag_id = Column(StringID(), primary_key=True, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    dataset = relationship('DatasetModel', back_populates="consuming_dags")
    queue_records = relationship(
        "DatasetDagRunQueue",
        primaryjoin="""and_(
            DagScheduleDatasetReference.dataset_id == foreign(DatasetDagRunQueue.dataset_id),
            DagScheduleDatasetReference.dag_id == foreign(DatasetDagRunQueue.target_dag_id),
        )""",
        cascade="all, delete, delete-orphan",
    )

    __tablename__ = "dag_schedule_dataset_reference"
    __table_args__ = (
        PrimaryKeyConstraint(dataset_id, dag_id, name="dsdr_pkey", mssql_clustered=True),
        ForeignKeyConstraint(
            (dataset_id,),
            ["dataset.id"],
            name='dsdr_dataset_fkey',
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            columns=(dag_id,),
            refcolumns=['dag.dag_id'],
            name='dsdr_dag_id_fkey',
            ondelete='CASCADE',
        ),
    )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.dataset_id == other.dataset_id and self.dag_id == other.dag_id
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.__mapper__.primary_key)

    def __repr__(self):
        args = []
        for attr in [x.name for x in self.__mapper__.primary_key]:
            args.append(f"{attr}={getattr(self, attr)!r}")
        return f"{self.__class__.__name__}({', '.join(args)})"


class TaskOutletDatasetReference(Base):
    """References from a task to a dataset that it updates / produces."""

    dataset_id = Column(Integer, primary_key=True, nullable=False)
    dag_id = Column(StringID(), primary_key=True, nullable=False)
    task_id = Column(StringID(), primary_key=True, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    dataset = relationship("DatasetModel", back_populates="producing_tasks")

    __tablename__ = "task_outlet_dataset_reference"
    __table_args__ = (
        ForeignKeyConstraint(
            (dataset_id,),
            ["dataset.id"],
            name='todr_dataset_fkey',
            ondelete="CASCADE",
        ),
        PrimaryKeyConstraint(dataset_id, dag_id, task_id, name="todr_pkey", mssql_clustered=True),
        ForeignKeyConstraint(
            columns=(dag_id,),
            refcolumns=['dag.dag_id'],
            name='todr_dag_id_fkey',
            ondelete='CASCADE',
        ),
    )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (
                self.dataset_id == other.dataset_id
                and self.dag_id == other.dag_id
                and self.task_id == other.task_id
            )
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.__mapper__.primary_key)

    def __repr__(self):
        args = []
        for attr in [x.name for x in self.__mapper__.primary_key]:
            args.append(f"{attr}={getattr(self, attr)!r}")
        return f"{self.__class__.__name__}({', '.join(args)})"


class DatasetDagRunQueue(Base):
    """Model for storing dataset events that need processing."""

    dataset_id = Column(Integer, primary_key=True, nullable=False)
    target_dag_id = Column(StringID(), primary_key=True, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)

    __tablename__ = "dataset_dag_run_queue"
    __table_args__ = (
        PrimaryKeyConstraint(dataset_id, target_dag_id, name="datasetdagrunqueue_pkey", mssql_clustered=True),
        ForeignKeyConstraint(
            (dataset_id,),
            ["dataset.id"],
            name='ddrq_dataset_fkey',
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            (target_dag_id,),
            ["dag.dag_id"],
            name='ddrq_dag_fkey',
            ondelete="CASCADE",
        ),
    )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.dataset_id == other.dataset_id and self.target_dag_id == other.target_dag_id
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.__mapper__.primary_key)

    def __repr__(self):
        args = []
        for attr in [x.name for x in self.__mapper__.primary_key]:
            args.append(f"{attr}={getattr(self, attr)!r}")
        return f"{self.__class__.__name__}({', '.join(args)})"


association_table = Table(
    "dagrun_dataset_event",
    Base.metadata,
    Column("dag_run_id", ForeignKey("dag_run.id", ondelete="CASCADE"), primary_key=True),
    Column("event_id", ForeignKey("dataset_event.id", ondelete="CASCADE"), primary_key=True),
    Index("idx_dagrun_dataset_events_dag_run_id", "dag_run_id"),
    Index("idx_dagrun_dataset_events_event_id", "event_id"),
)


class DatasetEvent(Base):
    """
    A table to store datasets events.

    :param dataset_id: reference to DatasetModel record
    :param extra: JSON field for arbitrary extra info
    :param source_task_id: the task_id of the TI which updated the dataset
    :param source_dag_id: the dag_id of the TI which updated the dataset
    :param source_run_id: the run_id of the TI which updated the dataset
    :param source_map_index: the map_index of the TI which updated the dataset
    :param timestamp: the time the event was logged

    We use relationships instead of foreign keys so that dataset events are not deleted even
    if the foreign key object is.
    """

    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(Integer, nullable=False)
    extra = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={})
    source_task_id = Column(StringID(), nullable=True)
    source_dag_id = Column(StringID(), nullable=True)
    source_run_id = Column(StringID(), nullable=True)
    source_map_index = Column(Integer, nullable=True, server_default=text("-1"))
    timestamp = Column(UtcDateTime, default=timezone.utcnow, nullable=False)

    __tablename__ = "dataset_event"
    __table_args__ = (
        Index('idx_dataset_id_timestamp', dataset_id, timestamp),
        {'sqlite_autoincrement': True},  # ensures PK values not reused
    )

    created_dagruns = relationship(
        "DagRun",
        secondary=association_table,
        backref="consumed_dataset_events",
    )

    source_task_instance = relationship(
        "TaskInstance",
        primaryjoin="""and_(
            DatasetEvent.source_dag_id == foreign(TaskInstance.dag_id),
            DatasetEvent.source_run_id == foreign(TaskInstance.run_id),
            DatasetEvent.source_task_id == foreign(TaskInstance.task_id),
            DatasetEvent.source_map_index == foreign(TaskInstance.map_index),
        )""",
        viewonly=True,
        lazy="select",
        uselist=False,
    )
    source_dag_run = relationship(
        "DagRun",
        primaryjoin="""and_(
            DatasetEvent.source_dag_id == foreign(DagRun.dag_id),
            DatasetEvent.source_run_id == foreign(DagRun.run_id),
        )""",
        viewonly=True,
        lazy="select",
        uselist=False,
    )
    dataset = relationship(
        DatasetModel,
        primaryjoin="DatasetEvent.dataset_id == foreign(DatasetModel.id)",
        viewonly=True,
        lazy="select",
        uselist=False,
    )

    @property
    def uri(self):
        return self.dataset.uri

    def __repr__(self) -> str:
        args = []
        for attr in [
            'id',
            'dataset_id',
            'extra',
            'source_task_id',
            'source_dag_id',
            'source_run_id',
            'source_map_index',
        ]:
            args.append(f"{attr}={getattr(self, attr)!r}")
        return f"{self.__class__.__name__}({', '.join(args)})"
