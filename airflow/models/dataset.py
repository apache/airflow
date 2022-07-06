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
from urllib.parse import urlparse

from sqlalchemy import Column, ForeignKeyConstraint, Index, Integer, PrimaryKeyConstraint, String
from sqlalchemy.orm import relationship

from airflow.models.base import ID_LEN, Base, StringID
from airflow.utils import timezone
from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime


class Dataset(Base):
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
    extra = Column(ExtendedJSON, nullable=True)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    dag_references = relationship("DatasetDagRef", back_populates="dataset")
    task_references = relationship("DatasetTaskRef", back_populates="dataset")

    __tablename__ = "dataset"
    __table_args__ = (
        Index('idx_uri_unique', uri, unique=True),
        {'sqlite_autoincrement': True},  # ensures PK values not reused
    )

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
        if isinstance(other, self.__class__):
            return self.uri == other.uri
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.uri)

    def __repr__(self):
        return f"{self.__class__.__name__}(uri={self.uri!r}, extra={self.extra!r})"


class DatasetDagRef(Base):
    """References from a DAG to an upstream dataset."""

    dataset_id = Column(Integer, primary_key=True, nullable=False)
    dag_id = Column(String(ID_LEN), primary_key=True, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    dataset = relationship('Dataset')

    __tablename__ = "dataset_dag_ref"
    __table_args__ = (
        PrimaryKeyConstraint(dataset_id, dag_id, name="datasetdagref_pkey", mssql_clustered=True),
        ForeignKeyConstraint(
            (dataset_id,),
            ["dataset.id"],
            name='datasetdagref_dataset_fkey',
            ondelete="CASCADE",
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


class DatasetTaskRef(Base):
    """References from a task to a downstream dataset."""

    dataset_id = Column(Integer, primary_key=True, nullable=False)
    dag_id = Column(String(ID_LEN), primary_key=True, nullable=False)
    task_id = Column(String(ID_LEN), primary_key=True, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    dataset = relationship("Dataset", back_populates="task_references")

    __tablename__ = "dataset_task_ref"
    __table_args__ = (
        ForeignKeyConstraint(
            (dataset_id,),
            ["dataset.id"],
            name='datasettaskref_dataset_fkey',
            ondelete="CASCADE",
        ),
        PrimaryKeyConstraint(dataset_id, dag_id, task_id, name="datasettaskref_pkey", mssql_clustered=True),
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
