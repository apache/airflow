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
from typing import TYPE_CHECKING

from sqlalchemy import Column, String, select
from sqlalchemy.orm import joinedload

from airflow.models.base import Base, StringID
from airflow.models.dag_version import DagVersion
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

    from sqlalchemy.orm import Session

    from airflow.models import DagRun
    from airflow.models.dag import DAG
    from airflow.models.dagwarning import DagWarning
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.sdk.definitions.dagbag import DagBag


def _create_dag_warnings(dagbag: DagBag) -> Iterable[DagWarning]:
    from airflow.models.dagwarning import DagWarning, DagWarningType

    # None means this feature is not enabled. Empty set means we don't know about any pools at all!
    if dagbag.known_pools is None:
        return

    for dag_id, dag in dagbag.dags.items():
        pools = {task.pool for task in dag.tasks}
        if nonexistent_pools := pools - dagbag.known_pools:
            yield DagWarning(
                dag_id,
                DagWarningType.NONEXISTENT_POOL,
                f"Dag '{dag_id}' references non-existent pools: {sorted(nonexistent_pools)!r}",
            )


@provide_session
def sync_bag_to_db(
    dagbag: DagBag,
    bundle_name: str,
    bundle_version: str | None,
    *,
    session: Session = NEW_SESSION,
) -> None:
    """Save attributes about list of DAG to the DB."""
    import airflow.models.dag
    from airflow.dag_processing.collection import update_dag_parsing_results_in_db
    from airflow.serialization.serialized_objects import LazyDeserializedDAG, SerializedDAG

    dags = [
        dag
        if isinstance(dag, airflow.models.dag.DAG)
        else LazyDeserializedDAG(data=SerializedDAG.to_dict(dag))
        for dag in dagbag.dags.values()
    ]
    import_errors = {(bundle_name, rel_path): error for rel_path, error in dagbag.import_errors.items()}

    update_dag_parsing_results_in_db(
        bundle_name,
        bundle_version,
        dags,
        import_errors,
        set(_create_dag_warnings(dagbag)),
        session=session,
    )


class DBDagBag:
    """
    Internal class for retrieving and caching dags in the scheduler.

    :meta private:
    """

    def __init__(self, load_op_links: bool = True):
        self._dags: dict[str, DAG] = {}  # dag_version_id to dag
        self.load_op_links = load_op_links

    def _read_dag(self, serdag: SerializedDagModel) -> DAG | None:
        serdag.load_op_links = self.load_op_links
        if dag := serdag.dag:
            self._dags[serdag.dag_version_id] = dag
        return dag

    def _get_dag(self, version_id: str, session: Session) -> DAG | None:
        if dag := self._dags.get(version_id):
            return dag
        dag_version = session.get(DagVersion, version_id, options=[joinedload(DagVersion.serialized_dag)])
        if not dag_version:
            return None
        if not (serdag := dag_version.serialized_dag):
            return None
        return self._read_dag(serdag)

    @staticmethod
    def _version_from_dag_run(dag_run: DagRun, session: Session):
        if not dag_run.bundle_version:
            if dag_version := DagVersion.get_latest_version(dag_id=dag_run.dag_id, session=session):
                return dag_version
        return dag_run.created_dag_version

    def get_dag_for_run(self, dag_run: DagRun, session: Session) -> DAG | None:
        if version := self._version_from_dag_run(dag_run=dag_run, session=session):
            return self._get_dag(version_id=version.id, session=session)
        return None

    def iter_all_latest_version_dags(self, *, session: Session) -> Generator[DAG, None, None]:
        """Walk through all latest version dags available in the database."""
        from airflow.models.serialized_dag import SerializedDagModel

        for sdm in session.scalars(select(SerializedDagModel)):
            if dag := self._read_dag(sdm):
                yield dag

    def get_latest_version_of_dag(self, dag_id: str, *, session: Session) -> DAG | None:
        """Get the latest version of a dag by its id."""
        from airflow.models.serialized_dag import SerializedDagModel

        if not (serdag := SerializedDagModel.get(dag_id, session=session)):
            return None
        return self._read_dag(serdag)


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
    id = Column(String(32), primary_key=True, default=generate_md5_hash, onupdate=generate_md5_hash)

    bundle_name = Column(StringID(), nullable=False)
    # The location of the file containing the DAG object
    # Note: Do not depend on fileloc pointing to a file; in the case of a
    # packaged DAG, it will point to the subpath of the DAG within the
    # associated zip.
    relative_fileloc = Column(String(2000), nullable=False)

    def __init__(self, bundle_name: str, relative_fileloc: str) -> None:
        super().__init__()
        self.bundle_name = bundle_name
        self.relative_fileloc = relative_fileloc

    def __repr__(self) -> str:
        return f"<DagPriorityParsingRequest: bundle_name={self.bundle_name} relative_fileloc={self.relative_fileloc}>"


def __getattr__(name: str):
    # TODO (GH-53918): This is needed for compatibility. Can we change all imports to SDK instead?
    # We can't simply move this to lazy imports because that would make this module inaccessible.
    if name == "DagBag":
        from airflow.sdk.definitions.dagbag import DagBag

        globals()["DagBag"] = DagBag
        return DagBag
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
