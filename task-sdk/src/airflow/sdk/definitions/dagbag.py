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

import structlog

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowClusterPolicyError,
    AirflowClusterPolicySkipDag,
    AirflowClusterPolicyViolation,
    AirflowDagCycleException,
    AirflowDagDuplicatedIdException,
    AirflowException,
)
from airflow.listeners.listener import get_listener_manager
from airflow.sdk import timezone
from airflow.utils.docs import get_docs_url
from airflow.utils.file import (
    correct_maybe_zipped,
    get_unique_dag_module_name,
    list_py_file_paths,
    might_contain_dag,
)
from airflow.utils.timeout import timeout
from airflow.utils.types import NOTSET

if TYPE_CHECKING:
    from collections.abc import Generator

    from airflow.sdk import DAG
    from airflow.utils.types import ArgNotSet

log = structlog.get_logger(logger_name=__name__)


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


class DagBag:
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
    ):
        super().__init__()
        self.bundle_path = bundle_path
        include_examples = (
            include_examples
            if isinstance(include_examples, bool)
            else conf.getboolean("core", "LOAD_EXAMPLES")
        )
        safe_mode = (
            safe_mode if isinstance(safe_mode, bool) else conf.getboolean("core", "DAG_DISCOVERY_SAFE_MODE")
        )

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
                include_examples=include_examples,
                safe_mode=safe_mode,
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

    def get_dag(self, dag_id: str) -> DAG | None:
        """
        Get the DAG out of the dictionary.

        :param dag_id: DAG ID
        """
        return self.dags.get(dag_id)

    def process_file(self, filepath, only_if_updated=True, safe_mode=True) -> list[DAG]:
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
        except Exception:
            log.exception("Failed to process file.", file=filepath)
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
            log.error("Received SIGSEGV while processing file.", file=filepath)
            relative_filepath = self._get_relative_fileloc(filepath)
            self.import_errors[relative_filepath] = f"Received SIGSEGV while processing {filepath}."

        try:
            signal.signal(signal.SIGSEGV, handler)
        except ValueError:
            log.warning("SIGSEGV handler registration failed. Not in the main thread.")

        if not might_contain_dag(filepath, safe_mode):
            # Don't want to spam user with skip messages
            if not self.has_logged:
                self.has_logged = True
                log.info("File assumed to contain no dags. Skipping.", file=filepath)
            return []

        log.debug("Importing dag file.", file=filepath)
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
                log.exception("Failed to import dag file.", file=filepath)
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
                    log.warning("Found item at zip root.", item=zip_path.name, file=filepath)

                log.debug("Reading item from zip file.", item=zip_info.filename, file=filepath)

                if not might_contain_dag(zip_info.filename, safe_mode, current_zip_file):
                    # todo: create ignore list
                    # Don't want to spam user with skip messages
                    if not self.has_logged:
                        self.has_logged = True
                        log.info(
                            "Item in zip file assumed to contain no dags. Skipping.",
                            file=filepath,
                            item=zip_info.filename,
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
                    log.exception("Failed to import item in zip file.", file=filepath, item=zip_info.filename)
                    relative_fileloc = self._get_relative_fileloc(os.path.join(filepath, zip_info.filename))
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

    def _process_modules(self, filepath, mods, file_last_changed_on_disk) -> list[DAG]:
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
                self.bag_dag(dag=dag)
            except AirflowClusterPolicySkipDag:
                pass
            except Exception as e:
                log.exception("Failed to bag dag.", file=dag.fileloc)
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
        dag.check_cycle()  # throws if a task cycle is found

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
            log.exception("Dag policy check failed.")
            raise AirflowClusterPolicyError(e)

        try:
            prev_dag = self.dags.get(dag.dag_id)
            if prev_dag and prev_dag.fileloc != dag.fileloc:
                raise AirflowDagDuplicatedIdException(
                    dag_id=dag.dag_id,
                    incoming=dag.fileloc,
                    existing=self.dags[dag.dag_id].fileloc,
                )
            self.dags[dag.dag_id] = dag
            log.debug("Loaded dag.", dag=dag)
        except (AirflowDagCycleException, AirflowDagDuplicatedIdException):
            # There was an error in bagging the dag. Remove it from the list of dags
            log.exception("Exception bagging dag.", dag_id=dag.dag_id)
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
        log.info("Filling up dag bag from location.", path=dag_folder)
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
            except Exception:
                log.exception("Failed to collect dags from file.", file=filepath)

        self.dagbag_stats = sorted(stats, key=lambda x: x.duration, reverse=True)

    def dagbag_report(self):
        """Print a report around DagBag loading stats."""
        from tabulate import tabulate

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
