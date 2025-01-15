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
import traceback

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
import functools
from typing import Iterable, Collection, List, Mapping, Tuple, TYPE_CHECKING

from airflow import settings
from airflow.listeners.listener import get_listener_manager
from airflow.models.dag import DAG
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.retries import MAX_DB_RETRIES, run_with_db_retries
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import load_only, Session


@dataclass(frozen=True)
class IngestedDag:
    dag: SerializedDAG
    fetch_time: datetime
    dag_hash: str


@dataclass(frozen=True)
class DagMetadata:
    dag_id: str
    fileloc: str
    ingestion_time: datetime


class DagSource(metaclass=ABCMeta):
    @abstractmethod
    def load_dag(self, dag_id: str, session: Session = NEW_SESSION) -> IngestedDag | None:
        raise NotImplementedError()


class DagStore(DagSource, LoggingMixin):
    """
        Data source of ingested DAG.

        :param load_op_links: Load extra op links, which might contain user-defined code.
    """
    def __init__(self, load_op_links: bool = False):
        self.load_op_links = load_op_links

    @classmethod
    @provide_session
    def _sync_perm_for_dag(cls, dag: DAG, session: Session = NEW_SESSION):
        """Sync DAG specific permissions."""
        dag_id = dag.dag_id

        cls.logger().debug("Syncing DAG permissions: %s to the DB", dag_id)
        from airflow.www.security_appless import ApplessAirflowSecurityManager

        security_manager = ApplessAirflowSecurityManager(session=session)
        security_manager.sync_perm_for_dag(dag_id, dag.access_control)

    @provide_session
    def list_dags_metadata(self, *, processor_subdir: str | None = None, parsed_before: datetime | None = None,  session: Session = NEW_SESSION) -> Collection[DagMetadata]:
        """List all DAGs with optional filters."""
        from airflow.models.dag import DagModel

        query = select(DagModel).where(DagModel.is_active).options(load_only(DagModel.dag_id, DagModel.last_parsed_time, DagModel.fileloc))
        if processor_subdir:
            query = query.where(DagModel.processor_subdir == processor_subdir)
        if parsed_before:
            query = query.where(DagModel.last_parsed_time < parsed_before)
        dags = session.scalars(query)
        return [DagMetadata(dag.dag_id, dag.fileloc, dag.last_parsed_time) for dag in dags]


    @provide_session
    def load_dag(self, dag_id: str, session: Session = NEW_SESSION) -> IngestedDag | None:
        """
        Load the DAG from metadata DB, it it exists.

        :param dag_id: DAG ID
        """
        from airflow.models.serialized_dag import SerializedDagModel

        dag_model: SerializedDagModel | None = SerializedDagModel.get(dag_id, session)
        if not dag_model:
            return None
        fetch_time = timezone.utcnow()
        dag_model.load_op_links = self.load_op_links

        return IngestedDag(dag_model.dag, fetch_time, dag_model.dag_hash)

    @provide_session
    def store_dags(
        self,
        # TODO: Also accept DAG code provider
        dags: Collection[DAG],
        processor_subdir: str | None = None,
        session: Session = NEW_SESSION,
    ) -> dict[str, str]:
        """Save attributes about DAGs to the DB."""
        from airflow.configuration import conf
        from airflow.models.serialized_dag import SerializedDagModel

        def _serialize_dag_capturing_errors(dag: DAG, session: Session, processor_subdir: str | None) -> List[Tuple[str, str]]:
            """
            Try to serialize the dag to the DB, but make a note of any errors.

            We can't place them directly in import_errors, as this may be retried, and work the next time
            """
            try:
                # We can't use bulk_write_to_db as we want to capture each error individually
                dag_was_updated = SerializedDagModel.write_dag(
                    dag,
                    min_update_interval=settings.MIN_SERIALIZED_DAG_UPDATE_INTERVAL,
                    session=session,
                    processor_subdir=processor_subdir,
                )
                if dag_was_updated:
                    DagStore._sync_perm_for_dag(dag, session=session)
                return []
            except OperationalError:
                raise
            except Exception:
                self.log.exception("Failed to write serialized DAG: %s", dag.fileloc)
                dagbag_import_error_traceback_depth = conf.getint(
                    "core", "dagbag_import_error_traceback_depth"
                )
                return [(dag.dag_id, traceback.format_exc(limit=-dagbag_import_error_traceback_depth))]

        # Retry 'DAG.bulk_write_to_db' & 'SerializedDagModel.bulk_sync_to_db' in case
        # of any Operational Errors
        # In case of failures, provide_session handles rollback
        import_errors = {}
        for attempt in run_with_db_retries(logger=self.log):
            with attempt:
                serialize_errors = []
                self.log.debug(
                    "Running dagbag.sync_to_db with retries. Try %d of %d",
                    attempt.retry_state.attempt_number,
                    MAX_DB_RETRIES,
                )
                self.log.debug("Calling the DAG.bulk_sync_to_db method")
                try:
                    # Write Serialized DAGs to DB, capturing errors
                    for dag in dags:
                        serialize_errors.extend(
                            _serialize_dag_capturing_errors(dag, session, processor_subdir)
                        )

                    DAG.bulk_write_to_db(dags, processor_subdir=processor_subdir, session=session)
                except OperationalError:
                    session.rollback()
                    raise
                # Only now we are "complete" do we update import_errors - don't want to record errors from
                # previous failed attempts
                import_errors.update(dict(serialize_errors))

        return import_errors

    @provide_session
    def delete_dags(self, dag_ids: Iterable[str], session: Session = NEW_SESSION):
        from airflow.models.dag import DagModel
        from airflow.models.dagcode import DagCode
        from airflow.models.serialized_dag import SerializedDagModel

        ids_set = set(dag_ids)
        SerializedDagModel.remove_dags(
            dag_ids=ids_set,
            session=session,
        )

        DagModel.deactivate_dags(dag_ids=ids_set, session=session)

        def _collect_dags(subdir2fileloc: dict[str, list[str]], dag: DagModel):
            subdir2fileloc[dag.processor_subdir].append(dag.fileloc)
            return subdir2fileloc
        subdir2fileloc = functools.reduce(
            _collect_dags, 
            filter(
                lambda dag: dag.dag_id not in ids_set,
                DagModel.active_dag_filelocs(session=session)
            ),
            defaultdict(list)
        )
        for subdir in subdir2fileloc:
            DagCode.remove_deleted_code(
                alive_dag_filelocs=subdir2fileloc[subdir],
                processor_subdir=subdir,
                session=session,
            )
    
    @provide_session
    def delete_removed_entries_dags(self, alive_entry_filelocs: Collection[str], processor_subdir: str, filepath_prefix: str = "", session: Session = NEW_SESSION):
        from airflow.models.dag import DagModel
        from airflow.models.dagcode import DagCode
        from airflow.models.serialized_dag import SerializedDagModel

        SerializedDagModel.remove_deleted_dags(
            alive_dag_filelocs=alive_entry_filelocs,
            processor_subdir=processor_subdir,
            filepath_prefix=filepath_prefix,
            session=session,
        )
        DagModel.deactivate_deleted_dags(
            alive_dag_filelocs=alive_entry_filelocs,
            processor_subdir=processor_subdir,
            filepath_prefix=filepath_prefix,
            session=session,
        )
        DagCode.remove_deleted_code(
            alive_dag_filelocs=alive_entry_filelocs,
            processor_subdir=processor_subdir,
            filepath_prefix=filepath_prefix,
            session=session,
        )

    @provide_session
    def delete_removed_processor_subdir_dags(self, alive_processor_subdirs: Collection[str], session: Session = NEW_SESSION):
        from airflow.models.dag import DagModel

        removed_dags = session.scalars(select(DagModel).where(DagModel.is_active, DagModel.processor_subdir.notin_(alive_processor_subdirs)).options(load_only(DagModel.dag_id)))
        self.delete_dags(map(lambda dag: dag.dag_id, removed_dags))

    @provide_session
    def update_import_errors(self, import_errors: Mapping[str, str], processor_subdir: str, session: Session = NEW_SESSION):
        from airflow.models.errors import ParseImportError

        existing_import_error_files = [x.filename for x in session.query(ParseImportError.filename).all()]

        # Add the errors of the processed files
        for filename, stacktrace in import_errors.items():
            if filename in existing_import_error_files:
                session.query(ParseImportError).filter(ParseImportError.filename == filename).update(
                    {"filename": filename, "timestamp": timezone.utcnow(), "stacktrace": stacktrace},
                    synchronize_session="fetch",
                )
                # sending notification when an existing dag import error occurs
                get_listener_manager().hook.on_existing_dag_import_error(
                    filename=filename, stacktrace=stacktrace
                )
            else:
                session.add(
                    ParseImportError(
                        filename=filename,
                        timestamp=timezone.utcnow(),
                        stacktrace=stacktrace,
                        processor_subdir=processor_subdir,
                    )
                )
                # sending notification when a new dag import error occurs
                get_listener_manager().hook.on_new_dag_import_error(filename=filename, stacktrace=stacktrace)

    @provide_session
    def delete_excluded_import_errors(self, filenames_to_keep: Collection[str], processor_subdir: str, filepath_prefix: str = "", session: Session = NEW_SESSION):
        from airflow.models.errors import ParseImportError

        session.execute(
                delete(ParseImportError)
                .where(
                    ParseImportError.processor_subdir == processor_subdir,
                    ParseImportError.filename.notin_(filenames_to_keep),
                    ParseImportError.filename.startswith(filepath_prefix),
                )
                .execution_options(synchronize_session="fetch")
        )
    
    @provide_session
    def delete_excluded_subdir_import_errors(self, processor_subdirs_to_keep: Collection[str], session: Session = NEW_SESSION):
        from airflow.models.errors import ParseImportError

        session.execute(
                delete(ParseImportError)
                .where(ParseImportError.processor_subdir.notin_(processor_subdirs_to_keep))
                .execution_options(synchronize_session="fetch")
        )


class CachedDagStore(DagSource, LoggingMixin):
    def __init__(self, store: DagStore):
        super().__init__()

        self.store = store
        self.cached_dags: dict[str, IngestedDag] = {}

    @provide_session
    def load_dag(self, dag_id: str, session: Session = NEW_SESSION) -> IngestedDag | None:
        from airflow.models.serialized_dag import SerializedDagModel

        if dag_id not in self.cached_dags:
            return self._refresh_from_store(dag_id, session)

        # If DAG is in the cache, check the following
        # 1. if time has come to check if DAG is updated (controlled by min_serialized_dag_fetch_secs)
        # 2. check the last_updated and hash columns in SerializedDag table to see if
        # Serialized DAG is updated
        # 3. if (2) is yes, fetch the Serialized DAG.
        # 4. if (2) returns None (i.e. Serialized DAG is deleted), revoke from cache
        # if it exists and return None.
        ingested_dag = self.cached_dags[dag_id]
        min_serialized_dag_fetch_secs = timedelta(seconds=settings.MIN_SERIALIZED_DAG_FETCH_INTERVAL)
        if timezone.utcnow() > ingested_dag.fetch_time + min_serialized_dag_fetch_secs:
            sd_latest_version_and_updated_datetime = (
                    SerializedDagModel.get_latest_version_hash_and_updated_datetime(
                        dag_id=dag_id, session=session
                    )
                )
            if not sd_latest_version_and_updated_datetime:
                self.log.warning("Serialized DAG %s no longer exists", dag_id)
                del self.cached_dags[dag_id]
                return None

            sd_latest_version, sd_last_updated_datetime = sd_latest_version_and_updated_datetime
            if (
                sd_last_updated_datetime > ingested_dag.fetch_time
                        or sd_latest_version != ingested_dag.dag_hash
            ):
                return self._refresh_from_store(dag_id, session)
        return self.cached_dags[dag_id]

    def _refresh_from_store(self, dag_id: str, session: Session) -> IngestedDag | None:
        dag = self.store.load_dag(dag_id, session)
        if not dag:
            return None
        self.cached_dags[dag_id] = dag
        return dag
