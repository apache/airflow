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

import contextlib
import importlib
import importlib.machinery
import importlib.util
import os
import signal
import sys
import textwrap
import traceback
import warnings
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from tabulate import tabulate

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowClusterPolicyError,
    AirflowClusterPolicySkipDag,
    AirflowClusterPolicyViolation,
    AirflowDagDuplicatedIdException,
    AirflowException,
    UnknownExecutorException,
)
from airflow.executors.executor_loader import ExecutorLoader
from airflow.listeners import get_listener_manager
from airflow.serialization.definitions.notset import NOTSET, ArgNotSet, is_arg_set
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.utils.docs import get_docs_url
from airflow.utils.file import (
    correct_maybe_zipped,
    get_unique_dag_module_name,
    list_py_file_paths,
    might_contain_dag,
)
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy.orm import Session

    from airflow import DAG
    from airflow.models.dagwarning import DagWarning


@contextlib.contextmanager
def _capture_with_reraise() -> Generator[list[warnings.WarningMessage], None, None]:
    """Capture warnings in context and re-raise it on exit from the context manager."""
    captured_warnings = []
    try:
        with warnings.catch_warnings(record=True) as captured_warnings:
            yield captured_warnings
    finally:
        if captured_warnings:
            for cw in captured_warnings:
                warnings.warn_explicit(
                    message=cw.message,
                    category=cw.category,
                    filename=cw.filename,
                    lineno=cw.lineno,
                    source=cw.source,
                )


class FileLoadStat(NamedTuple):
    """
    Information about single file.

    :param file: Loaded file.
    :param duration: Time spent on process file.
    :param dag_num: Total number of DAGs loaded in this file.
    :param task_num: Total number of Tasks loaded in this file.
    :param dags: DAGs names loaded in this file.
    :param warning_num: Total number of warnings captured from processing this file.
    """

    file: str
    duration: timedelta
    dag_num: int
    task_num: int
    dags: str
    warning_num: int


@contextlib.contextmanager
def timeout(seconds=1, error_message="Timeout"):
    import logging

    log = logging.getLogger(__name__)
    error_message = error_message + ", PID: " + str(os.getpid())

    def handle_timeout(signum, frame):
        """Log information and raises AirflowTaskTimeout."""
        log.error("Process timed out, PID: %s", str(os.getpid()))
        from airflow.sdk.exceptions import AirflowTaskTimeout

        raise AirflowTaskTimeout(error_message)

    try:
        try:
            signal.signal(signal.SIGALRM, handle_timeout)
            signal.setitimer(signal.ITIMER_REAL, seconds)
        except ValueError:
            log.warning("timeout can't be used in the current context", exc_info=True)
        yield
    finally:
        with contextlib.suppress(ValueError):
            signal.setitimer(signal.ITIMER_REAL, 0)


def _executor_exists(executor_name: str, team_name: str | None) -> bool:
    """Check if executor exists, with global fallback for teams."""
    try:
        # First pass check for team-specific executor or a global executor (i.e. team_name=None)
        ExecutorLoader.lookup_executor_name_by_str(executor_name, team_name=team_name)
        return True
    except UnknownExecutorException:
        if team_name:
            # If we had a team_name but didn't find an executor, check if there is a global executor that
            # satisfies the request.
            try:
                ExecutorLoader.lookup_executor_name_by_str(executor_name, team_name=None)
                return True
            except UnknownExecutorException:
                pass
    return False


def _validate_executor_fields(dag: DAG, bundle_name: str | None = None) -> None:
    """Validate that executors specified in tasks are available and owned by the same team as the dag bundle."""
    import logging

    log = logging.getLogger(__name__)
    dag_team_name = None

    # Check if multi team is available by reading the multi_team configuration (which is boolean)
    if conf.getboolean("core", "multi_team"):
        # Get team name from bundle configuration if available
        if bundle_name:
            from airflow.dag_processing.bundles.manager import DagBundlesManager

            bundle_manager = DagBundlesManager()
            bundle_config = bundle_manager._bundle_config[bundle_name]

            dag_team_name = bundle_config.team_name
            if dag_team_name:
                log.debug(
                    "Found team '%s' for DAG '%s' via bundle '%s'", dag_team_name, dag.dag_id, bundle_name
                )

    for task in dag.tasks:
        if not task.executor:
            continue

        if not _executor_exists(task.executor, dag_team_name):
            if dag_team_name:
                raise UnknownExecutorException(
                    f"Task '{task.task_id}' specifies executor '{task.executor}', which is not available "
                    f"for team '{dag_team_name}' (the team associated with DAG '{dag.dag_id}') or as a global executor. "
                    f"Make sure '{task.executor}' is configured for team '{dag_team_name}' or globally in your "
                    "[core] executors configuration, or update the task's executor to use one of the "
                    f"configured executors for team '{dag_team_name}' or available global executors."
                )
            raise UnknownExecutorException(
                f"Task '{task.task_id}' specifies executor '{task.executor}', which is not available. "
                "Make sure it is listed in your [core] executors configuration, or update the task's "
                "executor to use one of the configured executors."
            )


class DagBag(LoggingMixin):
    """
    A dagbag is a collection of dags, parsed out of a folder tree and has high level configuration settings.

    Some possible setting are database to use as a backend and what executor
    to use to fire off tasks. This makes it easier to run distinct environments
    for say production and development, tests, or for different teams or security
    profiles. What would have been system level settings are now dagbag level so
    that one system can run multiple, independent settings sets.

    :param dag_folder: the folder to scan to find DAGs
    :param include_examples: whether to include the examples that ship
        with airflow or not
    :param safe_mode: when ``False``, scans all python modules for dags.
        When ``True`` uses heuristics (files containing ``DAG`` and ``airflow`` strings)
        to filter python modules to scan for dags.
    :param load_op_links: Should the extra operator link be loaded via plugins when
        de-serializing the DAG? This flag is set to False in Scheduler so that Extra Operator links
        are not loaded to not run User code in Scheduler.
    :param collect_dags: when True, collects dags during class initialization.
    :param known_pools: If not none, then generate warnings if a Task attempts to use an unknown pool.
    """

    def __init__(
        self,
        dag_folder: str | Path | None = None,  # todo AIP-66: rename this to path
        include_examples: bool | ArgNotSet = NOTSET,
        safe_mode: bool | ArgNotSet = NOTSET,
        load_op_links: bool = True,
        collect_dags: bool = True,
        known_pools: set[str] | None = None,
        bundle_path: Path | None = None,
        bundle_name: str | None = None,
    ):
        super().__init__()
        self.bundle_path = bundle_path
        self.bundle_name = bundle_name

        dag_folder = dag_folder or settings.DAGS_FOLDER
        self.dag_folder = dag_folder
        self.dags: dict[str, DAG] = {}
        # the file's last modified timestamp when we last read it
        self.file_last_changed: dict[str, datetime] = {}
        # Store import errors with relative file paths as keys (relative to bundle_path)
        self.import_errors: dict[str, str] = {}
        self.captured_warnings: dict[str, tuple[str, ...]] = {}
        self.has_logged = False
        # Only used by SchedulerJob to compare the dag_hash to identify change in DAGs
        self.dags_hash: dict[str, str] = {}

        self.known_pools = known_pools

        self.dagbag_import_error_tracebacks = conf.getboolean("core", "dagbag_import_error_tracebacks")
        self.dagbag_import_error_traceback_depth = conf.getint("core", "dagbag_import_error_traceback_depth")
        if collect_dags:
            self.collect_dags(
                dag_folder=dag_folder,
                include_examples=(
                    include_examples
                    if is_arg_set(include_examples)
                    else conf.getboolean("core", "LOAD_EXAMPLES")
                ),
                safe_mode=(
                    safe_mode if is_arg_set(safe_mode) else conf.getboolean("core", "DAG_DISCOVERY_SAFE_MODE")
                ),
            )
        # Should the extra operator link be loaded via plugins?
        # This flag is set to False in Scheduler so that Extra Operator links are not loaded
        self.load_op_links = load_op_links

    def size(self) -> int:
        """:return: the amount of dags contained in this dagbag"""
        return len(self.dags)

    @property
    def dag_ids(self) -> list[str]:
        """
        Get DAG ids.

        :return: a list of DAG IDs in this bag
        """
        return list(self.dags)

    @provide_session
    def get_dag(self, dag_id, session: Session = NEW_SESSION):
        """
        Get the DAG out of the dictionary, and refreshes it if expired.

        :param dag_id: DAG ID
        """
        # Avoid circular import
        from airflow.models.dag import DagModel

        dag = self.dags.get(dag_id)

        # If DAG Model is absent, we can't check last_expired property. Is the DAG not yet synchronized?
        if (orm_dag := DagModel.get_current(dag_id, session=session)) is None:
            return dag

        is_expired = (
            orm_dag.last_expired and dag and dag.last_loaded and dag.last_loaded < orm_dag.last_expired
        )
        if is_expired:
            # Remove associated dags so we can re-add them.
            self.dags.pop(dag_id, None)
        if dag is None or is_expired:
            # Reprocess source file.
            found_dags = self.process_file(
                filepath=correct_maybe_zipped(orm_dag.fileloc), only_if_updated=False
            )

            # If the source file no longer exports `dag_id`, delete it from self.dags
            if found_dags and dag_id in [found_dag.dag_id for found_dag in found_dags]:
                return self.dags[dag_id]
            self.dags.pop(dag_id, None)
        return self.dags.get(dag_id)

    def process_file(self, filepath, only_if_updated=True, safe_mode=True):
        """Given a path to a python module or zip file, import the module and look for dag objects within."""
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        # if the source file no longer exists in the DB or in the filesystem,
        # return an empty list
        # todo: raise exception?

        if filepath is None or not os.path.isfile(filepath):
            return []

        try:
            # This failed before in what may have been a git sync
            # race condition
            file_last_changed_on_disk = datetime.fromtimestamp(os.path.getmtime(filepath))
            if (
                only_if_updated
                and filepath in self.file_last_changed
                and file_last_changed_on_disk == self.file_last_changed[filepath]
            ):
                return []
        except Exception as e:
            self.log.exception(e)
            return []

        # Ensure we don't pick up anything else we didn't mean to
        DagContext.autoregistered_dags.clear()

        self.captured_warnings.pop(filepath, None)
        with _capture_with_reraise() as captured_warnings:
            if filepath.endswith(".py") or not zipfile.is_zipfile(filepath):
                mods = self._load_modules_from_file(filepath, safe_mode)
            else:
                mods = self._load_modules_from_zip(filepath, safe_mode)

        if captured_warnings:
            formatted_warnings = []
            for msg in captured_warnings:
                category = msg.category.__name__
                if (module := msg.category.__module__) != "builtins":
                    category = f"{module}.{category}"
                formatted_warnings.append(f"{msg.filename}:{msg.lineno}: {category}: {msg.message}")
            self.captured_warnings[filepath] = tuple(formatted_warnings)

        found_dags = self._process_modules(filepath, mods, file_last_changed_on_disk)

        self.file_last_changed[filepath] = file_last_changed_on_disk
        return found_dags

    @property
    def dag_warnings(self) -> set[DagWarning]:
        """Get the set of DagWarnings for the bagged dags."""
        from airflow.models.dagwarning import DagWarning, DagWarningType

        # None means this feature is not enabled. Empty set means we don't know about any pools at all!
        if self.known_pools is None:
            return set()

        def get_pools(dag) -> dict[str, set[str]]:
            return {dag.dag_id: {task.pool for task in dag.tasks}}

        pool_dict: dict[str, set[str]] = {}
        for dag in self.dags.values():
            pool_dict.update(get_pools(dag))

        warnings: set[DagWarning] = set()
        for dag_id, dag_pools in pool_dict.items():
            nonexistent_pools = dag_pools - self.known_pools
            if nonexistent_pools:
                warnings.add(
                    DagWarning(
                        dag_id,
                        DagWarningType.NONEXISTENT_POOL,
                        f"Dag '{dag_id}' references non-existent pools: {sorted(nonexistent_pools)!r}",
                    )
                )
        return warnings

    def _get_relative_fileloc(self, filepath: str) -> str:
        """
        Get the relative file location for a given filepath.

        :param filepath: Absolute path to the file
        :return: Relative path from bundle_path, or original filepath if no bundle_path
        """
        if self.bundle_path:
            return str(Path(filepath).relative_to(self.bundle_path))
        return filepath

    def _load_modules_from_file(self, filepath, safe_mode):
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        def handler(signum, frame):
            """Handle SIGSEGV signal and let the user know that the import failed."""
            msg = f"Received SIGSEGV signal while processing {filepath}."
            self.log.error(msg)
            relative_filepath = self._get_relative_fileloc(filepath)
            self.import_errors[relative_filepath] = msg

        try:
            signal.signal(signal.SIGSEGV, handler)
        except ValueError:
            self.log.warning("SIGSEGV signal handler registration failed. Not in the main thread")

        if not might_contain_dag(filepath, safe_mode):
            # Don't want to spam user with skip messages
            if not self.has_logged:
                self.has_logged = True
                self.log.info("File %s assumed to contain no DAGs. Skipping.", filepath)
            return []

        self.log.debug("Importing %s", filepath)
        mod_name = get_unique_dag_module_name(filepath)

        if mod_name in sys.modules:
            del sys.modules[mod_name]

        DagContext.current_autoregister_module_name = mod_name

        def parse(mod_name, filepath):
            try:
                loader = importlib.machinery.SourceFileLoader(mod_name, filepath)
                spec = importlib.util.spec_from_loader(mod_name, loader)
                new_module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = new_module
                loader.exec_module(new_module)
                return [new_module]
            except KeyboardInterrupt:
                # re-raise ctrl-c
                raise
            except BaseException as e:
                # Normally you shouldn't catch BaseException, but in this case we want to, as, pytest.skip
                # raises an exception which does not inherit from Exception, and we want to catch that here.
                # This would also catch `exit()` in a dag file
                DagContext.autoregistered_dags.clear()
                self.log.exception("Failed to import: %s", filepath)
                relative_filepath = self._get_relative_fileloc(filepath)
                if self.dagbag_import_error_tracebacks:
                    self.import_errors[relative_filepath] = traceback.format_exc(
                        limit=-self.dagbag_import_error_traceback_depth
                    )
                else:
                    self.import_errors[relative_filepath] = str(e)
                return []

        dagbag_import_timeout = settings.get_dagbag_import_timeout(filepath)

        if not isinstance(dagbag_import_timeout, (int, float)):
            raise TypeError(
                f"Value ({dagbag_import_timeout}) from get_dagbag_import_timeout must be int or float"
            )

        if dagbag_import_timeout <= 0:  # no parsing timeout
            return parse(mod_name, filepath)

        timeout_msg = (
            f"DagBag import timeout for {filepath} after {dagbag_import_timeout}s.\n"
            "Please take a look at these docs to improve your DAG import time:\n"
            f"* {get_docs_url('best-practices.html#top-level-python-code')}\n"
            f"* {get_docs_url('best-practices.html#reducing-dag-complexity')}"
        )
        with timeout(dagbag_import_timeout, error_message=timeout_msg):
            return parse(mod_name, filepath)

    def _load_modules_from_zip(self, filepath, safe_mode):
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        mods = []
        with zipfile.ZipFile(filepath) as current_zip_file:
            for zip_info in current_zip_file.infolist():
                zip_path = Path(zip_info.filename)
                if zip_path.suffix not in [".py", ".pyc"] or len(zip_path.parts) > 1:
                    continue

                if zip_path.stem == "__init__":
                    self.log.warning("Found %s at root of %s", zip_path.name, filepath)

                self.log.debug("Reading %s from %s", zip_info.filename, filepath)

                if not might_contain_dag(zip_info.filename, safe_mode, current_zip_file):
                    # todo: create ignore list
                    # Don't want to spam user with skip messages
                    if not self.has_logged:
                        self.has_logged = True
                        self.log.info(
                            "File %s:%s assumed to contain no DAGs. Skipping.", filepath, zip_info.filename
                        )
                    continue

                mod_name = zip_path.stem
                if mod_name in sys.modules:
                    del sys.modules[mod_name]

                DagContext.current_autoregister_module_name = mod_name
                try:
                    sys.path.insert(0, filepath)
                    current_module = importlib.import_module(mod_name)
                    mods.append(current_module)
                except Exception as e:
                    DagContext.autoregistered_dags.clear()
                    fileloc = os.path.join(filepath, zip_info.filename)
                    self.log.exception("Failed to import: %s", fileloc)
                    relative_fileloc = self._get_relative_fileloc(fileloc)
                    if self.dagbag_import_error_tracebacks:
                        self.import_errors[relative_fileloc] = traceback.format_exc(
                            limit=-self.dagbag_import_error_traceback_depth
                        )
                    else:
                        self.import_errors[relative_fileloc] = str(e)
                finally:
                    if sys.path[0] == filepath:
                        del sys.path[0]
        return mods

    def _process_modules(self, filepath, mods, file_last_changed_on_disk):
        from airflow.sdk import DAG
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        top_level_dags = {(o, m) for m in mods for o in m.__dict__.values() if isinstance(o, DAG)}

        top_level_dags.update(DagContext.autoregistered_dags)

        DagContext.current_autoregister_module_name = None
        DagContext.autoregistered_dags.clear()

        found_dags = []

        for dag, mod in top_level_dags:
            dag.fileloc = mod.__file__
            relative_fileloc = self._get_relative_fileloc(dag.fileloc)
            dag.relative_fileloc = relative_fileloc
            try:
                dag.validate()
                _validate_executor_fields(dag, self.bundle_name)
                self.bag_dag(dag=dag)
            except AirflowClusterPolicySkipDag:
                pass
            except Exception as e:
                self.log.exception("Failed to bag_dag: %s", dag.fileloc)
                self.import_errors[relative_fileloc] = f"{type(e).__name__}: {e}"
                self.file_last_changed[dag.fileloc] = file_last_changed_on_disk
            else:
                found_dags.append(dag)
        return found_dags

    def bag_dag(self, dag: DAG):
        """
        Add the DAG into the bag.

        :raises: AirflowDagCycleException if a cycle is detected.
        :raises: AirflowDagDuplicatedIdException if this dag already exists in the bag.
        """
        dag.check_cycle()  # throws exception if a task cycle is found

        dag.resolve_template_files()
        dag.last_loaded = timezone.utcnow()

        try:
            # Check policies
            settings.dag_policy(dag)

            for task in dag.tasks:
                # The listeners are not supported when ending a task via a trigger on asynchronous operators.
                if getattr(task, "end_from_trigger", False) and get_listener_manager().has_listeners:
                    raise AirflowException(
                        "Listeners are not supported with end_from_trigger=True for deferrable operators. "
                        "Task %s in DAG %s has end_from_trigger=True with listeners from plugins. "
                        "Set end_from_trigger=False to use listeners.",
                        task.task_id,
                        dag.dag_id,
                    )

                settings.task_policy(task)
        except (AirflowClusterPolicyViolation, AirflowClusterPolicySkipDag):
            raise
        except Exception as e:
            self.log.exception(e)
            raise AirflowClusterPolicyError(e)
        from airflow.sdk.exceptions import AirflowDagCycleException

        try:
            prev_dag = self.dags.get(dag.dag_id)
            if prev_dag and prev_dag.fileloc != dag.fileloc:
                raise AirflowDagDuplicatedIdException(
                    dag_id=dag.dag_id,
                    incoming=dag.fileloc,
                    existing=self.dags[dag.dag_id].fileloc,
                )
            self.dags[dag.dag_id] = dag
            self.log.debug("Loaded DAG %s", dag)
        except (AirflowDagCycleException, AirflowDagDuplicatedIdException):
            # There was an error in bagging the dag. Remove it from the list of dags
            self.log.exception("Exception bagging dag: %s", dag.dag_id)
            raise

    def collect_dags(
        self,
        dag_folder: str | Path | None = None,
        only_if_updated: bool = True,
        include_examples: bool = conf.getboolean("core", "LOAD_EXAMPLES"),
        safe_mode: bool = conf.getboolean("core", "DAG_DISCOVERY_SAFE_MODE"),
    ):
        """
        Look for python modules in a given path, import them, and add them to the dagbag collection.

        Note that if a ``.airflowignore`` file is found while processing
        the directory, it will behave much like a ``.gitignore``,
        ignoring files that match any of the patterns specified
        in the file.

        **Note**: The patterns in ``.airflowignore`` are interpreted as either
        un-anchored regexes or gitignore-like glob expressions, depending on
        the ``DAG_IGNORE_FILE_SYNTAX`` configuration parameter.
        """
        self.log.info("Filling up the DagBag from %s", dag_folder)
        dag_folder = dag_folder or self.dag_folder
        # Used to store stats around DagBag processing
        stats = []

        # Ensure dag_folder is a str -- it may have been a pathlib.Path
        dag_folder = correct_maybe_zipped(str(dag_folder))

        files_to_parse = list_py_file_paths(dag_folder, safe_mode=safe_mode)

        if include_examples:
            from airflow import example_dags

            example_dag_folder = next(iter(example_dags.__path__))

            files_to_parse.extend(list_py_file_paths(example_dag_folder, safe_mode=safe_mode))

        for filepath in files_to_parse:
            try:
                file_parse_start_dttm = timezone.utcnow()
                found_dags = self.process_file(filepath, only_if_updated=only_if_updated, safe_mode=safe_mode)

                file_parse_end_dttm = timezone.utcnow()
                stats.append(
                    FileLoadStat(
                        file=filepath.replace(settings.DAGS_FOLDER, ""),
                        duration=file_parse_end_dttm - file_parse_start_dttm,
                        dag_num=len(found_dags),
                        task_num=sum(len(dag.tasks) for dag in found_dags),
                        dags=str([dag.dag_id for dag in found_dags]),
                        warning_num=len(self.captured_warnings.get(filepath, [])),
                    )
                )
            except Exception as e:
                self.log.exception(e)

        self.dagbag_stats = sorted(stats, key=lambda x: x.duration, reverse=True)

    def dagbag_report(self):
        """Print a report around DagBag loading stats."""
        stats = self.dagbag_stats
        dag_folder = self.dag_folder
        duration = sum((o.duration for o in stats), timedelta()).total_seconds()
        dag_num = sum(o.dag_num for o in stats)
        task_num = sum(o.task_num for o in stats)
        table = tabulate(stats, headers="keys")

        report = textwrap.dedent(
            f"""\n
        -------------------------------------------------------------------
        DagBag loading stats for {dag_folder}
        -------------------------------------------------------------------
        Number of DAGs: {dag_num}
        Total task number: {task_num}
        DagBag parsing time: {duration}\n{table}
        """
        )
        return report


@provide_session
def sync_bag_to_db(
    dagbag: DagBag,
    bundle_name: str,
    bundle_version: str | None,
    *,
    session: Session = NEW_SESSION,
) -> None:
    """Save attributes about list of DAG to the DB."""
    from airflow.dag_processing.collection import update_dag_parsing_results_in_db

    import_errors = {(bundle_name, rel_path): error for rel_path, error in dagbag.import_errors.items()}

    # Build the set of all files that were parsed and include files with import errors
    # in case they are not in file_last_changed
    files_parsed = set(import_errors)
    if dagbag.bundle_path:
        files_parsed.update(
            (bundle_name, dagbag._get_relative_fileloc(abs_filepath))
            for abs_filepath in dagbag.file_last_changed
        )

    update_dag_parsing_results_in_db(
        bundle_name,
        bundle_version,
        [LazyDeserializedDAG.from_dag(dag) for dag in dagbag.dags.values()],
        import_errors,
        None,  # file parsing duration is not well defined when parsing multiple files / multiple DAGs.
        dagbag.dag_warnings,
        session=session,
        files_parsed=files_parsed,
    )
