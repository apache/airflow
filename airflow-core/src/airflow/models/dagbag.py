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

import hashlib
from typing import TYPE_CHECKING, Any

from sqlalchemy import String, inspect, select
from sqlalchemy.orm import Mapped, Session, joinedload
from sqlalchemy.orm.attributes import NO_VALUE

from airflow.models import DagRun
from airflow.models.base import Base, StringID
from airflow.models.dag_version import DagVersion
from airflow.utils.sqlalchemy import mapped_column

if TYPE_CHECKING:
    from collections.abc import Generator

    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.serialization.definitions.dag import SerializedDAG


class DBDagBag:
    """
    Internal class for retrieving and caching dags in the scheduler.

    :meta private:
    """

    def __init__(self, load_op_links: bool = True) -> None:
        self._dags: dict[str, SerializedDAG] = {}  # dag_version_id to dag
        self.load_op_links = load_op_links

    def _read_dag(self, serdag: SerializedDagModel) -> SerializedDAG | None:
        serdag.load_op_links = self.load_op_links
        if dag := serdag.dag:
            self._dags[serdag.dag_version_id] = dag
        return dag

    def _get_dag(self, version_id: str, session: Session) -> SerializedDAG | None:
        if dag := self._dags.get(version_id):
            return dag
        dag_version = session.get(DagVersion, version_id, options=[joinedload(DagVersion.serialized_dag)])
        if not dag_version:
            return None
        if not (serdag := dag_version.serialized_dag):
            return None
        return self._read_dag(serdag)

    @staticmethod
    def _version_from_dag_run(dag_run: DagRun, *, session: Session) -> DagVersion | None:
        if not dag_run.bundle_version:
            if dag_version := DagVersion.get_latest_version(dag_id=dag_run.dag_id, session=session):
                return dag_version

        # Check if created_dag_version relationship is already loaded to avoid DetachedInstanceError
        info: Any = inspect(dag_run)
        if info.attrs.created_dag_version.loaded_value is not NO_VALUE:
            # Relationship is already loaded, safe to access
            return dag_run.created_dag_version

        # Relationship not loaded, fetch it explicitly from current session
        return session.get(DagVersion, dag_run.created_dag_version_id)

    def get_dag_for_run(self, dag_run: DagRun, session: Session) -> SerializedDAG | None:
        if version := self._version_from_dag_run(dag_run=dag_run, session=session):
            return self._get_dag(version_id=version.id, session=session)
        return None

    def iter_all_latest_version_dags(self, *, session: Session) -> Generator[SerializedDAG, None, None]:
        """Walk through all latest version dags available in the database."""
        from airflow.models.serialized_dag import SerializedDagModel

        for sdm in session.scalars(select(SerializedDagModel)):
            if dag := self._read_dag(sdm):
                yield dag

    def get_latest_version_of_dag(self, dag_id: str, *, session: Session) -> SerializedDAG | None:
        """Get the latest version of a dag by its id."""
        from airflow.models.serialized_dag import SerializedDagModel

        if not (serdag := SerializedDagModel.get(dag_id, session=session)):
            return None
        return self._read_dag(serdag)

    def get_dag(self, dag_id: str, run_id: str, *, session: Session) -> SerializedDAG | None:
        dag_run = DagRun.find_duplicate(dag_id=dag_id, run_id=run_id, session=session)
        if dag_run:
            return self.get_dag_for_run(dag_run=dag_run, session=session)
        return None


def generate_md5_hash(context):
    bundle_name = context.get_current_parameters()["bundle_name"]
    relative_fileloc = context.get_current_parameters()["relative_fileloc"]
    return hashlib.md5(f"{bundle_name}:{relative_fileloc}".encode()).hexdigest()


class DagPriorityParsingRequest(Base):
    """Model to store the dag parsing requests that will be prioritized when parsing files."""

    __tablename__ = "dag_priority_parsing_request"

    # Adding a unique constraint to fileloc results in the creation of an index and we have a limitation
    # on the size of the string we can use in the index for MySQL DB. We also have to keep the fileloc
    # size consistent with other tables. This is a workaround to enforce the unique constraint.
    id: Mapped[str] = mapped_column(
        String(32), primary_key=True, default=generate_md5_hash, onupdate=generate_md5_hash
    )

    bundle_name: Mapped[str] = mapped_column(StringID(), nullable=False)
    # The location of the file containing the DAG object
    # Note: Do not depend on fileloc pointing to a file; in the case of a
    # packaged DAG, it will point to the subpath of the DAG within the
    # associated zip.
    relative_fileloc: Mapped[str] = mapped_column(String(2000), nullable=False)

    def __init__(self, bundle_name: str, relative_fileloc: str) -> None:
        super().__init__()
        self.bundle_name = bundle_name
        self.relative_fileloc = relative_fileloc

    def __repr__(self) -> str:
        return f"<DagPriorityParsingRequest: bundle_name={self.bundle_name} relative_fileloc={self.relative_fileloc}>"


def __getattr__(name: str) -> Any:
    """
    Backwards-compat shim: importing DagBag from airflow.models.dagbag is deprecated.

    Emits DeprecationWarning and re-exports DagBag from airflow.dag_processing.dagbag
    to preserve compatibility for external callers.
    """
    if name in {"DagBag", "FileLoadStat", "timeout"}:
        import warnings

        from airflow.utils.deprecation_tools import DeprecatedImportWarning

        warnings.warn(
            f"Importing {name} from airflow.models.dagbag is deprecated and will be removed in a future "
            "release. Please import from airflow.dag_processing.dagbag instead.",
            DeprecatedImportWarning,
            stacklevel=2,
        )
        # Import on demand to avoid import-time side effects
        from airflow.dag_processing import dagbag as _dagbag

        return getattr(_dagbag, name)
    raise AttributeError(name)
