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
"""Serialized DAG table in database."""

from __future__ import annotations

import logging
import zlib
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

import sqlalchemy_jsonfield
import uuid6
from sqlalchemy import ForeignKey, LargeBinary, String, select, tuple_, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, backref, foreign, joinedload, relationship
from sqlalchemy.sql.expression import func, literal
from sqlalchemy_utils import UUIDType

from airflow._shared.timezones import timezone
from airflow.models.asset import (
    AssetAliasModel,
    AssetModel,
)
from airflow.models.base import ID_LEN, Base
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagcode import DagCode
from airflow.models.dagrun import DagRun
from airflow.sdk.definitions.asset import AssetUniqueKey
from airflow.serialization.dag_dependency import DagDependency
from airflow.serialization.serialized_objects import LazyDeserializedDAG, SerializedDAG
from airflow.settings import COMPRESS_SERIALIZED_DAGS, json
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, get_dialect_name, mapped_column

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    from sqlalchemy.sql.elements import ColumnElement


log = logging.getLogger(__name__)


class _DagDependenciesResolver:
    """Resolver that resolves dag dependencies to include asset id and assets link to asset aliases."""

    def __init__(self, dag_id_dependencies: Sequence[tuple[str, dict]], session: Session) -> None:
        self.dag_id_dependencies = dag_id_dependencies
        self.session = session

        self.asset_key_to_id: dict[AssetUniqueKey, int] = {}
        self.asset_ref_name_to_asset_id_name: dict[str, tuple[int, str]] = {}
        self.asset_ref_uri_to_asset_id_name: dict[str, tuple[int, str]] = {}
        self.alias_names_to_asset_ids_names: dict[str, list[tuple[int, str]]] = {}

    def resolve(self) -> dict[str, list[DagDependency]]:
        asset_names_uris, asset_ref_names, asset_ref_uris, asset_alias_names = self.collect_asset_info()

        self.asset_key_to_id = self.collect_asset_key_to_ids(asset_names_uris)
        self.asset_ref_name_to_asset_id_name = self.collect_asset_name_ref_to_ids_names(asset_ref_names)
        self.asset_ref_uri_to_asset_id_name = self.collect_asset_uri_ref_to_ids_names(asset_ref_uris)
        self.alias_names_to_asset_ids_names = self.collect_alias_to_assets(asset_alias_names)

        dag_depdendencies_by_dag: dict[str, list[DagDependency]] = {}
        for dag_id, deps_data in self.dag_id_dependencies:
            dag_deps: list[DagDependency] = []
            for dep_data in deps_data or {}:
                dep_type = dep_data["dependency_type"]
                if dep_type == "asset":
                    dag_deps.append(self.resolve_asset_dag_dep(dep_data))
                elif dep_type == "asset-name-ref":
                    dag_deps.extend(self.resolve_asset_name_ref_dag_dep(dep_data))
                elif dep_type == "asset-uri-ref":
                    dag_deps.extend(self.resolve_asset_uri_ref_dag_dep(dep_data))
                elif dep_type == "asset-alias":
                    dag_deps.extend(self.resolve_asset_alias_dag_dep(dep_data))
                else:
                    # Replace asset_key with asset id if it's in source or target
                    for node_key in ("source", "target"):
                        if dep_data[node_key].startswith("asset:"):
                            unique_key = AssetUniqueKey.from_str(dep_data[node_key].split(":")[1])
                            asset_id = self.asset_key_to_id[unique_key]
                            dep_data[node_key] = f"asset:{asset_id}"
                            break

                    dep_id = dep_data["dependency_id"]
                    dag_deps.append(
                        DagDependency(
                            source=dep_data["source"],
                            target=dep_data["target"],
                            # handle the case that serialized_dag does not have label column (e.g., from 2.x)
                            label=dep_data.get("label", dep_id),
                            dependency_type=dep_data["dependency_type"],
                            dependency_id=dep_id,
                        )
                    )

            dag_depdendencies_by_dag[dag_id] = dag_deps
        return dag_depdendencies_by_dag

    def collect_asset_info(self) -> tuple[set, set, set, set]:
        asset_names_uris: set[tuple[str, str]] = set()
        asset_ref_names: set[str] = set()
        asset_ref_uris: set[str] = set()
        asset_alias_names: set[str] = set()
        for _, deps_data in self.dag_id_dependencies:
            for dep_data in deps_data or {}:
                dep_type = dep_data["dependency_type"]
                dep_id = dep_data["dependency_id"]
                if dep_type == "asset":
                    unique_key = AssetUniqueKey.from_str(dep_id)
                    asset_names_uris.add((unique_key.name, unique_key.uri))
                elif dep_type == "asset-name-ref":
                    asset_ref_names.add(dep_id)
                elif dep_type == "asset-uri-ref":
                    asset_ref_uris.add(dep_id)
                elif dep_type == "asset-alias":
                    asset_alias_names.add(dep_id)
        return asset_names_uris, asset_ref_names, asset_ref_uris, asset_alias_names

    def collect_asset_key_to_ids(self, asset_name_uris: set[tuple[str, str]]) -> dict[AssetUniqueKey, int]:
        return {
            AssetUniqueKey(name=name, uri=uri): asset_id
            for name, uri, asset_id in self.session.execute(
                select(AssetModel.name, AssetModel.uri, AssetModel.id).where(
                    tuple_(AssetModel.name, AssetModel.uri).in_(asset_name_uris)
                )
            )
        }

    def collect_asset_name_ref_to_ids_names(self, asset_ref_names: set[str]) -> dict[str, tuple[int, str]]:
        return {
            name: (asset_id, name)
            for name, asset_id in self.session.execute(
                select(AssetModel.name, AssetModel.id).where(
                    AssetModel.name.in_(asset_ref_names), AssetModel.active.has()
                )
            )
        }

    def collect_asset_uri_ref_to_ids_names(self, asset_ref_uris: set[str]) -> dict[str, tuple[int, str]]:
        return {
            uri: (asset_id, name)
            for uri, name, asset_id in self.session.execute(
                select(AssetModel.uri, AssetModel.name, AssetModel.id).where(
                    AssetModel.uri.in_(asset_ref_uris), AssetModel.active.has()
                )
            )
        }

    def collect_alias_to_assets(self, asset_alias_names: set[str]) -> dict[str, list[tuple[int, str]]]:
        return {
            aam.name: [(am.id, am.name) for am in aam.assets]
            for aam in self.session.scalars(
                select(AssetAliasModel).where(AssetAliasModel.name.in_(asset_alias_names))
            )
        }

    def resolve_asset_dag_dep(self, dep_data: dict) -> DagDependency:
        dep_id = dep_data["dependency_id"]
        unique_key = AssetUniqueKey.from_str(dep_id)
        return DagDependency(
            source=dep_data["source"],
            target=dep_data["target"],
            # handle the case that serialized_dag does not have label column (e.g., from 2.x)
            label=dep_data.get("label", unique_key.name),
            dependency_type=dep_data["dependency_type"],
            dependency_id=str(self.asset_key_to_id[unique_key]),
        )

    def resolve_asset_ref_dag_dep(
        self, dep_data: dict, ref_type: Literal["asset-name-ref", "asset-uri-ref"]
    ) -> Iterator[DagDependency]:
        if ref_type == "asset-name-ref":
            ref_to_asset_id_name = self.asset_ref_name_to_asset_id_name
        elif ref_type == "asset-uri-ref":
            ref_to_asset_id_name = self.asset_ref_uri_to_asset_id_name
        else:
            raise ValueError(
                f"ref_type {ref_type} is invalid. It should be either asset-name-ref or asset-uri-ref"
            )

        dep_id = dep_data["dependency_id"]
        is_source_ref = dep_data["source"] == ref_type
        if dep_id in ref_to_asset_id_name:
            # The asset ref can be resolved into a valid asset
            asset_id, asset_name = ref_to_asset_id_name[dep_id]
            yield DagDependency(
                source="asset" if is_source_ref else dep_data["source"],
                target=dep_data["target"] if is_source_ref else "asset",
                label=asset_name,
                dependency_type="asset",
                dependency_id=str(asset_id),
            )
        else:
            yield DagDependency(
                source=dep_data["source"],
                target=dep_data["target"],
                # handle the case that serialized_dag does not have label column (e.g., from 2.x)
                label=dep_data.get("label", dep_id),
                dependency_type=dep_data["dependency_type"],
                dependency_id=dep_id,
            )

    def resolve_asset_name_ref_dag_dep(self, dep_data: dict) -> Iterator[DagDependency]:
        return self.resolve_asset_ref_dag_dep(dep_data=dep_data, ref_type="asset-name-ref")

    def resolve_asset_uri_ref_dag_dep(self, dep_data: dict) -> Iterator[DagDependency]:
        return self.resolve_asset_ref_dag_dep(dep_data=dep_data, ref_type="asset-uri-ref")

    def resolve_asset_alias_dag_dep(self, dep_data: dict) -> Iterator[DagDependency]:
        dep_id = dep_data["dependency_id"]
        assets = self.alias_names_to_asset_ids_names[dep_id]
        if assets:
            for asset_id, asset_name in assets:
                is_source_alias = dep_data["source"] == "asset-alias"
                # asset
                yield DagDependency(
                    source="asset" if is_source_alias else f"asset-alias:{dep_id}",
                    target=f"asset-alias:{dep_id}" if is_source_alias else "asset",
                    label=asset_name,
                    dependency_type="asset",
                    dependency_id=str(asset_id),
                )
                # asset alias
                yield DagDependency(
                    source=f"asset:{asset_id}" if is_source_alias else dep_data["source"],
                    target=dep_data["target"] if is_source_alias else f"asset:{asset_id}",
                    label=dep_id,
                    dependency_type="asset-alias",
                    dependency_id=dep_id,
                )
        else:
            yield DagDependency(
                source=dep_data["source"],
                target=dep_data["target"],
                # handle the case that serialized_dag does not have label column (e.g., from 2.x)
                label=dep_data.get("label", dep_id),
                dependency_type=dep_data["dependency_type"],
                dependency_id=dep_id,
            )


class SerializedDagModel(Base):
    """
    A table for serialized DAGs.

    serialized_dag table is a snapshot of DAG files synchronized by scheduler.
    This feature is controlled by:

    * ``[core] min_serialized_dag_update_interval = 30`` (s):
      serialized DAGs are updated in DB when a file gets processed by scheduler,
      to reduce DB write rate, there is a minimal interval of updating serialized DAGs.
    * ``[dag_processor] refresh_interval = 300`` (s):
      interval of deleting serialized DAGs in DB when the files are deleted, suggest
      to use a smaller interval such as 60
    * ``[core] compress_serialized_dags``:
      whether compressing the dag data to the Database.

    It is used by webserver to load dags
    because reading from database is lightweight compared to importing from files,
    it solves the webserver scalability issue.
    """

    __tablename__ = "serialized_dag"
    id: Mapped[str] = mapped_column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)
    dag_id: Mapped[str] = mapped_column(String(ID_LEN), nullable=False)
    _data: Mapped[dict | None] = mapped_column(
        "data", sqlalchemy_jsonfield.JSONField(json=json).with_variant(JSONB, "postgresql"), nullable=True
    )
    _data_compressed: Mapped[bytes | None] = mapped_column("data_compressed", LargeBinary, nullable=True)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, default=timezone.utcnow)
    last_updated: Mapped[datetime] = mapped_column(
        UtcDateTime, nullable=False, default=timezone.utcnow, onupdate=timezone.utcnow
    )
    dag_hash: Mapped[str] = mapped_column(String(32), nullable=False)

    dag_runs = relationship(
        DagRun,
        primaryjoin=dag_id == foreign(DagRun.dag_id),  # type: ignore[has-type]
        backref=backref("serialized_dag", uselist=False, innerjoin=True),
    )

    dag_model = relationship(
        DagModel,
        primaryjoin=dag_id == DagModel.dag_id,  # type: ignore[has-type]
        foreign_keys=dag_id,
        uselist=False,
        innerjoin=True,
        backref=backref("serialized_dag", uselist=False, innerjoin=True),
    )
    dag_version_id: Mapped[str] = mapped_column(
        UUIDType(binary=False),
        ForeignKey("dag_version.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    dag_version = relationship("DagVersion", back_populates="serialized_dag")

    load_op_links = True

    def __init__(self, dag: LazyDeserializedDAG) -> None:
        self.dag_id = dag.dag_id
        dag_data = dag.data
        self.dag_hash = SerializedDagModel.hash(dag_data)

        # partially ordered json data
        dag_data_json = json.dumps(dag_data, sort_keys=True).encode("utf-8")

        if COMPRESS_SERIALIZED_DAGS:
            self._data = None
            self._data_compressed = zlib.compress(dag_data_json)
        else:
            self._data = dag_data
            self._data_compressed = None

        # serve as cache so no need to decompress and load, when accessing data field
        # when COMPRESS_SERIALIZED_DAGS is True
        self.__data_cache = dag_data

    def __repr__(self) -> str:
        return f"<SerializedDag: {self.dag_id}>"

    @classmethod
    def hash(cls, dag_data):
        """Hash the data to get the dag_hash."""
        dag_data = cls._sort_serialized_dag_dict(dag_data)
        data_ = dag_data.copy()
        # Remove fileloc from the hash so changes to fileloc
        # does not affect the hash. In 3.0+, a combination of
        # bundle_path and relative fileloc more correctly determines the
        # dag file location.
        data_["dag"].pop("fileloc", None)
        data_json = json.dumps(data_, sort_keys=True).encode("utf-8")
        return md5(data_json).hexdigest()

    @classmethod
    def _sort_serialized_dag_dict(cls, serialized_dag: Any):
        """Recursively sort json_dict and its nested dictionaries and lists."""
        if isinstance(serialized_dag, dict):
            return {k: cls._sort_serialized_dag_dict(v) for k, v in sorted(serialized_dag.items())}
        if isinstance(serialized_dag, list):
            if all(isinstance(i, dict) for i in serialized_dag):
                if all(
                    isinstance(i.get("__var", {}), Iterable) and "task_id" in i.get("__var", {})
                    for i in serialized_dag
                ):
                    return sorted(
                        [cls._sort_serialized_dag_dict(i) for i in serialized_dag],
                        key=lambda x: x["__var"]["task_id"],
                    )
            elif all(isinstance(item, str) for item in serialized_dag):
                return sorted(serialized_dag)
            return [cls._sort_serialized_dag_dict(i) for i in serialized_dag]
        return serialized_dag

    @classmethod
    @provide_session
    def write_dag(
        cls,
        dag: LazyDeserializedDAG,
        bundle_name: str,
        bundle_version: str | None = None,
        min_update_interval: int | None = None,
        session: Session = NEW_SESSION,
    ) -> bool:
        """
        Serialize a DAG and writes it into database.

        If the record already exists, it checks if the Serialized DAG changed or not. If it is
        changed, it updates the record, ignores otherwise.

        :param dag: a DAG to be written into database
        :param bundle_name: bundle name of the DAG
        :param bundle_version: bundle version of the DAG
        :param min_update_interval: minimal interval in seconds to update serialized DAG
        :param session: ORM Session

        :returns: Boolean indicating if the DAG was written to the DB
        """
        # Checks if (Current Time - Time when the DAG was written to DB) < min_update_interval
        # If Yes, does nothing
        # If No or the DAG does not exists, updates / writes Serialized DAG to DB
        if min_update_interval is not None:
            if session.scalar(
                select(literal(True))
                .where(
                    cls.dag_id == dag.dag_id,
                    (timezone.utcnow() - timedelta(seconds=min_update_interval)) < cls.last_updated,
                )
                .select_from(cls)
            ):
                return False

        log.debug("Checking if DAG (%s) changed", dag.dag_id)
        new_serialized_dag = cls(dag)
        serialized_dag_hash = session.scalars(
            select(cls.dag_hash).where(cls.dag_id == dag.dag_id).order_by(cls.created_at.desc())
        ).first()
        dag_version = session.scalar(
            select(DagVersion)
            .where(DagVersion.dag_id == dag.dag_id)
            .options(joinedload(DagVersion.task_instances))
            .order_by(DagVersion.created_at.desc())
            .limit(1)
        )

        if (
            serialized_dag_hash == new_serialized_dag.dag_hash
            and dag_version
            and dag_version.bundle_name == bundle_name
        ):
            log.debug("Serialized DAG (%s) is unchanged. Skipping writing to DB", dag.dag_id)
            return False

        if dag_version and not dag_version.task_instances:
            # This is for dynamic DAGs that the hashes changes often. We should update
            # the serialized dag, the dag_version and the dag_code instead of a new version
            # if the dag_version is not associated with any task instances

            # Use direct UPDATE to avoid loading the full serialized DAG
            result = session.execute(
                update(cls)
                .where(cls.dag_version_id == dag_version.id)
                .values(
                    {
                        cls._data: new_serialized_dag._data,
                        cls._data_compressed: new_serialized_dag._data_compressed,
                        cls.dag_hash: new_serialized_dag.dag_hash,
                    }
                )
            )

            if getattr(result, "rowcount", 0) == 0:
                # No rows updated - serialized DAG doesn't exist
                return False
            # The dag_version and dag_code may not have changed, still we should
            # do the below actions:
            # Update the latest dag version
            dag_version.bundle_name = bundle_name
            dag_version.bundle_version = bundle_version
            session.merge(dag_version)
            # Update the latest DagCode
            DagCode.update_source_code(dag_id=dag.dag_id, fileloc=dag.fileloc, session=session)
            return True

        dagv = DagVersion.write_dag(
            dag_id=dag.dag_id,
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            session=session,
        )
        log.debug("Writing Serialized DAG: %s to the DB", dag.dag_id)
        new_serialized_dag.dag_version = dagv
        session.add(new_serialized_dag)
        log.debug("DAG: %s written to the DB", dag.dag_id)
        DagCode.write_code(dagv, dag.fileloc, session=session)
        return True

    @classmethod
    def latest_item_select_object(cls, dag_id):
        from airflow.settings import engine

        if engine.dialect.name == "mysql":
            # Prevent "Out of sort memory" caused by large values in cls.data column for MySQL.
            # Details in https://github.com/apache/airflow/pull/55589
            latest_item_id = (
                select(cls.id).where(cls.dag_id == dag_id).order_by(cls.created_at.desc()).limit(1)
            )
            return select(cls).where(cls.id == latest_item_id)
        return select(cls).where(cls.dag_id == dag_id).order_by(cls.created_at.desc()).limit(1)

    @classmethod
    @provide_session
    def get_latest_serialized_dags(
        cls, *, dag_ids: list[str], session: Session = NEW_SESSION
    ) -> Sequence[SerializedDagModel]:
        """
        Get the latest serialized dags of given DAGs.

        :param dag_ids: The list of DAG IDs.
        :param session: The database session.
        :return: The latest serialized dag of the DAGs.
        """
        # Subquery to get the latest serdag per dag_id
        latest_serdag_subquery = (
            select(cls.dag_id, func.max(cls.created_at).label("created_at"))
            .where(cls.dag_id.in_(dag_ids))
            .group_by(cls.dag_id)
            .subquery()
        )
        latest_serdags = session.scalars(
            select(cls)
            .join(
                latest_serdag_subquery,
                cls.created_at == latest_serdag_subquery.c.created_at,
            )
            .where(cls.dag_id.in_(dag_ids))
        ).all()
        return latest_serdags or []

    @classmethod
    @provide_session
    def read_all_dags(cls, session: Session = NEW_SESSION) -> dict[str, SerializedDAG]:
        """
        Read all DAGs in serialized_dag table.

        :param session: ORM Session
        :returns: a dict of DAGs read from database
        """
        latest_serialized_dag_subquery = (
            select(cls.dag_id, func.max(cls.created_at).label("max_created")).group_by(cls.dag_id).subquery()
        )
        serialized_dags = session.scalars(
            select(cls).join(
                latest_serialized_dag_subquery,
                (cls.dag_id == latest_serialized_dag_subquery.c.dag_id)
                and (cls.created_at == latest_serialized_dag_subquery.c.max_created),
            )
        )

        dags = {}
        for row in serialized_dags:
            log.debug("Deserializing DAG: %s", row.dag_id)
            dag = row.dag

            # Coherence check
            if dag.dag_id == row.dag_id:
                dags[row.dag_id] = dag
            else:
                log.warning(
                    "dag_id Mismatch in DB: Row with dag_id '%s' has Serialised DAG with '%s' dag_id",
                    row.dag_id,
                    dag.dag_id,
                )
        return dags

    @property
    def data(self) -> dict | None:
        # use __data_cache to avoid decompress and loads
        if not hasattr(self, "_SerializedDagModel__data_cache") or self.__data_cache is None:
            if self._data_compressed:
                self.__data_cache = json.loads(zlib.decompress(self._data_compressed))
            else:
                self.__data_cache = self._data

        return self.__data_cache

    @property
    def dag(self) -> SerializedDAG:
        """The DAG deserialized from the ``data`` column."""
        SerializedDAG._load_operator_extra_links = self.load_op_links
        if isinstance(self.data, dict):
            data = self.data
        elif isinstance(self.data, str):
            data = json.loads(self.data)
        else:
            raise ValueError("invalid or missing serialized DAG data")
        return SerializedDAG.from_dict(data)

    @classmethod
    @provide_session
    def has_dag(cls, dag_id: str, session: Session = NEW_SESSION) -> bool:
        """
        Check a DAG exist in serialized_dag table.

        :param dag_id: the DAG to check
        :param session: ORM Session
        """
        return session.scalar(select(literal(True)).where(cls.dag_id == dag_id).limit(1)) is not None

    @classmethod
    @provide_session
    def get_dag(cls, dag_id: str, session: Session = NEW_SESSION) -> SerializedDAG | None:
        row = cls.get(dag_id, session=session)
        if row:
            return row.dag
        return None

    @classmethod
    @provide_session
    def get(cls, dag_id: str, session: Session = NEW_SESSION) -> SerializedDagModel | None:
        """
        Get the SerializedDAG for the given dag ID.

        :param dag_id: the DAG to fetch
        :param session: ORM Session
        """
        return session.scalar(cls.latest_item_select_object(dag_id))

    @classmethod
    @provide_session
    def get_dag_dependencies(cls, session: Session = NEW_SESSION) -> dict[str, list[DagDependency]]:
        """
        Get the dependencies between DAGs.

        :param session: ORM Session
        """
        load_json: Callable
        data_col_to_select: ColumnElement[Any] | InstrumentedAttribute[bytes | None]
        if COMPRESS_SERIALIZED_DAGS is False:
            dialect = get_dialect_name(session)
            if dialect in ["sqlite", "mysql"]:
                data_col_to_select = func.json_extract(cls._data, "$.dag.dag_dependencies")

                def load_json(deps_data):
                    return json.loads(deps_data) if deps_data else []
            elif dialect == "postgresql":
                # Use #> operator which works for both JSON and JSONB types
                # Returns the JSON sub-object at the specified path
                data_col_to_select = cls._data.op("#>")(literal('{"dag","dag_dependencies"}'))
                load_json = lambda x: x
            else:
                data_col_to_select = func.json_extract_path(cls._data, "dag", "dag_dependencies")
                load_json = lambda x: x
        else:
            data_col_to_select = cls._data_compressed

            def load_json(deps_data):
                return json.loads(zlib.decompress(deps_data))["dag"]["dag_dependencies"] if deps_data else []

        latest_sdag_subquery = (
            select(cls.dag_id, func.max(cls.created_at).label("max_created")).group_by(cls.dag_id).subquery()
        )
        query = session.execute(
            select(cls.dag_id, data_col_to_select)
            .join(
                latest_sdag_subquery,
                (cls.dag_id == latest_sdag_subquery.c.dag_id)
                & (cls.created_at == latest_sdag_subquery.c.max_created),
            )
            .join(cls.dag_model)
            .where(~DagModel.is_stale)
        )
        dag_depdendencies = [(str(dag_id), load_json(deps_data)) for dag_id, deps_data in query]
        resolver = _DagDependenciesResolver(dag_id_dependencies=dag_depdendencies, session=session)
        dag_depdendencies_by_dag = resolver.resolve()
        return dag_depdendencies_by_dag
