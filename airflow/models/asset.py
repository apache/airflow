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

from urllib.parse import urlsplit

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

from airflow.assets import Asset, AssetAlias
from airflow.models.base import Base, StringID
from airflow.settings import json
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime

alias_association_table = Table(
    "asset_alias_asset",
    Base.metadata,
    Column("alias_id", ForeignKey("asset_alias.id", ondelete="CASCADE"), primary_key=True),
    Column("dataset_id", ForeignKey("dataset.id", ondelete="CASCADE"), primary_key=True),
    Index("idx_asset_alias_asset_alias_id", "alias_id"),
    Index("idx_asset_alias_asset_asset_id", "dataset_id"),
    ForeignKeyConstraint(
        ("alias_id",),
        ["asset_alias.id"],
        name="a_aa_alias_id",
        ondelete="CASCADE",
    ),
    ForeignKeyConstraint(
        ("dataset_id",),
        ["dataset.id"],
        name="a_aa_asset_id",
        ondelete="CASCADE",
    ),
)

asset_alias_asset_event_assocation_table = Table(
    "asset_alias_asset_event",
    Base.metadata,
    Column("alias_id", ForeignKey("asset_alias.id", ondelete="CASCADE"), primary_key=True),
    Column("event_id", ForeignKey("asset_event.id", ondelete="CASCADE"), primary_key=True),
    Index("idx_asset_alias_asset_event_alias_id", "alias_id"),
    Index("idx_asset_alias_asset_event_event_id", "event_id"),
    ForeignKeyConstraint(
        ("alias_id",),
        ["asset_alias.id"],
        name="dss_de_alias_id",
        ondelete="CASCADE",
    ),
    ForeignKeyConstraint(
        ("event_id",),
        ["asset_event.id"],
        name="dss_de_event_id",
        ondelete="CASCADE",
    ),
)


class AssetAliasModel(Base):
    """
    A table to store asset alias.

    :param uri: a string that uniquely identifies the asset alias
    """

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(
        String(length=1500).with_variant(
            String(
                length=1500,
                # latin1 allows for more indexed length in mysql
                # and this field should only be ascii chars
                collation="latin1_general_cs",
            ),
            "mysql",
        ),
        nullable=False,
    )
    group = Column(
        String(length=1500).with_variant(
            String(
                length=1500,
                # latin1 allows for more indexed length in mysql
                # and this field should only be ascii chars
                collation="latin1_general_cs",
            ),
            "mysql",
        ),
        default=str,
        nullable=False,
    )

    __tablename__ = "asset_alias"
    __table_args__ = (
        Index("idx_asset_alias_name_unique", name, unique=True),
        {"sqlite_autoincrement": True},  # ensures PK values not reused
    )

    datasets = relationship(
        "AssetModel",
        secondary=alias_association_table,
        backref="aliases",
    )
    asset_events = relationship(
        "AssetEvent",
        secondary=asset_alias_asset_event_assocation_table,
        back_populates="source_aliases",
    )
    consuming_dags = relationship("DagScheduleAssetAliasReference", back_populates="asset_alias")

    @classmethod
    def from_public(cls, obj: AssetAlias) -> AssetAliasModel:
        return cls(name=obj.name)

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name!r})"

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, (self.__class__, AssetAlias)):
            return self.name == other.name
        else:
            return NotImplemented

    def to_public(self) -> AssetAlias:
        return AssetAlias(name=self.name)


class AssetModel(Base):
    """
    A table to store assets.

    :param uri: a string that uniquely identifies the asset
    :param extra: JSON field for arbitrary extra info
    """

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(
        String(length=1500).with_variant(
            String(
                length=1500,
                # latin1 allows for more indexed length in mysql
                # and this field should only be ascii chars
                collation="latin1_general_cs",
            ),
            "mysql",
        ),
        nullable=False,
    )
    uri = Column(
        String(length=1500).with_variant(
            String(
                length=1500,
                # latin1 allows for more indexed length in mysql
                # and this field should only be ascii chars
                collation="latin1_general_cs",
            ),
            "mysql",
        ),
        nullable=False,
    )
    group = Column(
        String(length=1500).with_variant(
            String(
                length=1500,
                # latin1 allows for more indexed length in mysql
                # and this field should only be ascii chars
                collation="latin1_general_cs",
            ),
            "mysql",
        ),
        default=str,
        nullable=False,
    )
    extra = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=False, default={})

    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    active = relationship("AssetActive", uselist=False, viewonly=True)

    consuming_dags = relationship("DagScheduleAssetReference", back_populates="asset")
    producing_tasks = relationship("TaskOutletAssetReference", back_populates="asset")

    __tablename__ = "dataset"
    __table_args__ = (
        Index("idx_asset_name_uri_unique", name, uri, unique=True),
        {"sqlite_autoincrement": True},  # ensures PK values not reused
    )

    @classmethod
    def from_public(cls, obj: Asset) -> AssetModel:
        return cls(name=obj.name, uri=obj.uri, group=obj.group, extra=obj.extra)

    def __init__(self, name: str = "", uri: str = "", **kwargs):
        if not name and not uri:
            raise TypeError("must provide either 'name' or 'uri'")
        elif not name:
            name = uri
        elif not uri:
            uri = name
        try:
            uri.encode("ascii")
        except UnicodeEncodeError:
            raise ValueError("URI must be ascii") from None
        parsed = urlsplit(uri)
        if parsed.scheme and parsed.scheme.lower() == "airflow":
            raise ValueError("Scheme 'airflow' is reserved.")
        super().__init__(name=name, uri=uri, **kwargs)

    def __eq__(self, other):
        if isinstance(other, (self.__class__, Asset)):
            return self.name == other.name and self.uri == other.uri
        return NotImplemented

    def __hash__(self):
        return hash((self.name, self.uri))

    def __repr__(self):
        return f"{self.__class__.__name__}(uri={self.uri!r}, extra={self.extra!r})"

    def to_public(self) -> Asset:
        return Asset(name=self.name, uri=self.uri, group=self.group, extra=self.extra)


class AssetActive(Base):
    """
    Collection of active assets.

    An asset is considered active if it is declared by the user in any DAG files.
    AssetModel entries that are not active (also called orphaned in some parts
    of the code base) are still kept in the database, but have their corresponding
    entries in this table removed. This ensures we keep all possible history on
    distinct assets (those with non-matching name-URI pairs), but still ensure
    *name and URI are each unique* within active assets.
    """

    name = Column(
        String(length=1500).with_variant(
            String(
                length=1500,
                # latin1 allows for more indexed length in mysql
                # and this field should only be ascii chars
                collation="latin1_general_cs",
            ),
            "mysql",
        ),
        nullable=False,
    )
    uri = Column(
        String(length=1500).with_variant(
            String(
                length=1500,
                # latin1 allows for more indexed length in mysql
                # and this field should only be ascii chars
                collation="latin1_general_cs",
            ),
            "mysql",
        ),
        nullable=False,
    )

    __tablename__ = "asset_active"
    __table_args__ = (
        PrimaryKeyConstraint(name, uri, name="asset_active_pkey"),
        ForeignKeyConstraint(
            columns=[name, uri],
            refcolumns=["dataset.name", "dataset.uri"],
            name="asset_active_asset_name_uri_fkey",
            ondelete="CASCADE",
        ),
        Index("idx_asset_active_name_unique", name, unique=True),
        Index("idx_asset_active_uri_unique", uri, unique=True),
    )

    @classmethod
    def for_asset(cls, asset: AssetModel) -> AssetActive:
        return cls(name=asset.name, uri=asset.uri)


class DagScheduleAssetAliasReference(Base):
    """References from a DAG to an asset alias of which it is a consumer."""

    alias_id = Column(Integer, primary_key=True, nullable=False)
    dag_id = Column(StringID(), primary_key=True, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    asset_alias = relationship("AssetAliasModel", back_populates="consuming_dags")
    dag = relationship("DagModel", back_populates="schedule_asset_alias_references")

    __tablename__ = "dag_schedule_asset_alias_reference"
    __table_args__ = (
        PrimaryKeyConstraint(alias_id, dag_id, name="dsdar_pkey"),
        ForeignKeyConstraint(
            (alias_id,),
            ["asset_alias.id"],
            name="dsaar_asset_alias_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            columns=(dag_id,),
            refcolumns=["dag.dag_id"],
            name="dsdar_dag_fkey",
            ondelete="CASCADE",
        ),
        Index("idx_dag_schedule_asset_alias_reference_dag_id", dag_id),
    )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.alias_id == other.alias_id and self.dag_id == other.dag_id
        return NotImplemented

    def __hash__(self):
        return hash(self.__mapper__.primary_key)

    def __repr__(self):
        args = [f"{x.name}={getattr(self, x.name)!r}" for x in self.__mapper__.primary_key]
        return f"{self.__class__.__name__}({', '.join(args)})"


class DagScheduleAssetReference(Base):
    """References from a DAG to an asset of which it is a consumer."""

    dataset_id = Column(Integer, primary_key=True, nullable=False)
    dag_id = Column(StringID(), primary_key=True, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    asset = relationship("AssetModel", back_populates="consuming_dags")
    dag = relationship("DagModel", back_populates="schedule_asset_references")

    queue_records = relationship(
        "AssetDagRunQueue",
        primaryjoin="""and_(
            DagScheduleAssetReference.dataset_id == foreign(AssetDagRunQueue.dataset_id),
            DagScheduleAssetReference.dag_id == foreign(AssetDagRunQueue.target_dag_id),
        )""",
        cascade="all, delete, delete-orphan",
    )

    __tablename__ = "dag_schedule_asset_reference"
    __table_args__ = (
        PrimaryKeyConstraint(dataset_id, dag_id, name="dsar_pkey"),
        ForeignKeyConstraint(
            (dataset_id,),
            ["dataset.id"],
            name="dsar_asset_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            columns=(dag_id,),
            refcolumns=["dag.dag_id"],
            name="dsar_dag_id_fkey",
            ondelete="CASCADE",
        ),
        Index("idx_dag_schedule_asset_reference_dag_id", dag_id),
    )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.dataset_id == other.dataset_id and self.dag_id == other.dag_id
        return NotImplemented

    def __hash__(self):
        return hash(self.__mapper__.primary_key)

    def __repr__(self):
        args = [f"{attr}={getattr(self, attr)!r}" for attr in [x.name for x in self.__mapper__.primary_key]]
        return f"{self.__class__.__name__}({', '.join(args)})"


class TaskOutletAssetReference(Base):
    """References from a task to an asset that it updates / produces."""

    dataset_id = Column(Integer, primary_key=True, nullable=False)
    dag_id = Column(StringID(), primary_key=True, nullable=False)
    task_id = Column(StringID(), primary_key=True, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    asset = relationship("AssetModel", back_populates="producing_tasks")

    __tablename__ = "task_outlet_asset_reference"
    __table_args__ = (
        ForeignKeyConstraint(
            (dataset_id,),
            ["dataset.id"],
            name="toar_asset_fkey",
            ondelete="CASCADE",
        ),
        PrimaryKeyConstraint(dataset_id, dag_id, task_id, name="toar_pkey"),
        ForeignKeyConstraint(
            columns=(dag_id,),
            refcolumns=["dag.dag_id"],
            name="toar_dag_id_fkey",
            ondelete="CASCADE",
        ),
        Index("idx_task_outlet_asset_reference_dag_id", dag_id),
    )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (
                self.dataset_id == other.dataset_id
                and self.dag_id == other.dag_id
                and self.task_id == other.task_id
            )

        return NotImplemented

    def __hash__(self):
        return hash(self.__mapper__.primary_key)

    def __repr__(self):
        args = []
        for attr in [x.name for x in self.__mapper__.primary_key]:
            args.append(f"{attr}={getattr(self, attr)!r}")
        return f"{self.__class__.__name__}({', '.join(args)})"


class AssetDagRunQueue(Base):
    """Model for storing asset events that need processing."""

    dataset_id = Column(Integer, primary_key=True, nullable=False)
    target_dag_id = Column(StringID(), primary_key=True, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    dataset = relationship("AssetModel", viewonly=True)

    __tablename__ = "asset_dag_run_queue"
    __table_args__ = (
        PrimaryKeyConstraint(dataset_id, target_dag_id, name="assetdagrunqueue_pkey"),
        ForeignKeyConstraint(
            (dataset_id,),
            ["dataset.id"],
            name="adrq_asset_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            (target_dag_id,),
            ["dag.dag_id"],
            name="adrq_dag_fkey",
            ondelete="CASCADE",
        ),
        Index("idx_asset_dag_run_queue_target_dag_id", target_dag_id),
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
    "dagrun_asset_event",
    Base.metadata,
    Column("dag_run_id", ForeignKey("dag_run.id", ondelete="CASCADE"), primary_key=True),
    Column("event_id", ForeignKey("asset_event.id", ondelete="CASCADE"), primary_key=True),
    Index("idx_dagrun_asset_events_dag_run_id", "dag_run_id"),
    Index("idx_dagrun_asset_events_event_id", "event_id"),
)


class AssetEvent(Base):
    """
    A table to store assets events.

    :param dataset_id: reference to AssetModel record
    :param extra: JSON field for arbitrary extra info
    :param source_task_id: the task_id of the TI which updated the asset
    :param source_dag_id: the dag_id of the TI which updated the asset
    :param source_run_id: the run_id of the TI which updated the asset
    :param source_map_index: the map_index of the TI which updated the asset
    :param timestamp: the time the event was logged

    We use relationships instead of foreign keys so that asset events are not deleted even
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

    __tablename__ = "asset_event"
    __table_args__ = (
        Index("idx_asset_id_timestamp", dataset_id, timestamp),
        {"sqlite_autoincrement": True},  # ensures PK values not reused
    )

    created_dagruns = relationship(
        "DagRun",
        secondary=association_table,
        backref="consumed_asset_events",
    )

    source_aliases = relationship(
        "AssetAliasModel",
        secondary=asset_alias_asset_event_assocation_table,
        back_populates="asset_events",
    )

    source_task_instance = relationship(
        "TaskInstance",
        primaryjoin="""and_(
            AssetEvent.source_dag_id == foreign(TaskInstance.dag_id),
            AssetEvent.source_run_id == foreign(TaskInstance.run_id),
            AssetEvent.source_task_id == foreign(TaskInstance.task_id),
            AssetEvent.source_map_index == foreign(TaskInstance.map_index),
        )""",
        viewonly=True,
        lazy="select",
        uselist=False,
    )
    source_dag_run = relationship(
        "DagRun",
        primaryjoin="""and_(
            AssetEvent.source_dag_id == foreign(DagRun.dag_id),
            AssetEvent.source_run_id == foreign(DagRun.run_id),
        )""",
        viewonly=True,
        lazy="select",
        uselist=False,
    )
    dataset = relationship(
        AssetModel,
        primaryjoin="AssetEvent.dataset_id == foreign(AssetModel.id)",
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
            "id",
            "dataset_id",
            "extra",
            "source_task_id",
            "source_dag_id",
            "source_run_id",
            "source_map_index",
            "source_aliases",
        ]:
            args.append(f"{attr}={getattr(self, attr)!r}")
        return f"{self.__class__.__name__}({', '.join(args)})"
