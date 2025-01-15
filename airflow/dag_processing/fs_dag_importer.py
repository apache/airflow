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

import datetime
from dataclasses import dataclass
import functools
import importlib
import os
import pathlib
import sys
import traceback
from typing import TYPE_CHECKING, Collection, Generator, Mapping, Tuple
import zipfile

from airflow import settings
from airflow.configuration import conf
from airflow.dag_processing.dag_importer import DagsImportResult, DagImporter, ImportOptions, PathData
from airflow.exceptions import (
    AirflowClusterPolicyError,
    AirflowClusterPolicySkipDag,
    AirflowClusterPolicyViolation,
    AirflowDagCycleException,
    AirflowDagDuplicatedIdException,
    AirflowException,
    AirflowTaskTimeout,
)
from airflow.listeners.listener import get_listener_manager
from airflow.utils import timezone
from airflow.utils.dag_cycle_tester import check_cycle
from airflow.utils.docs import get_docs_url
from airflow.utils.file import (
    correct_maybe_zipped,
    get_unique_dag_module_name,
    list_py_file_paths,
    might_contain_dag,
)
from airflow.utils.timeout import timeout
from airflow.utils.warnings import capture_with_reraise

if TYPE_CHECKING:
    from types import ModuleType
    from airflow.models.dag import DAG


@dataclass(frozen=True)
class FileLoadStat:
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
    duration: datetime.timedelta
    dag_num: int
    task_num: int
    dags: str
    warning_num: int


class FSDagImporter(DagImporter):
    def __init__(self):
        # TODO: add mtime-based cache. Keep expiration from get_dag in mind.
        super().__init__()
        # For backwards compatibility.
        self.safe_mode = conf.getboolean("core", "DAG_DISCOVERY_SAFE_MODE")
        self.dagbag_import_error_tracebacks = conf.getboolean("core", "dagbag_import_error_tracebacks")
        self.dagbag_import_error_traceback_depth = conf.getint("core", "dagbag_import_error_traceback_depth")

    def import_path(self, dagpath: str, options: ImportOptions | None = None) -> Generator[DagsImportResult, None, None]:
        # TODO: Support import options, for ex. skip_unchanged.
        if not options:
            options = ImportOptions()
        yield self._collect_dags(dagpath)
    
    def list_paths(self, subpath: str) -> Iterable[PathData]:
        return map(lambda path: PathData(path), self._list_py_file_paths(subpath))

    def _collect_dags(
        self,
        dag_folder: str,
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

        self.log.info("Importing DAGs from %s", dag_folder)
        # Used to store stats around DagBag processing
        stats = []

        import_results = []
        for filepath in self._list_py_file_paths(str(dag_folder)):
            try:
                file_parse_start_dttm = timezone.utcnow()
                result = self._process_file(filepath)
                if not result:
                    continue
                import_results.append(result)

                file_parse_end_dttm = timezone.utcnow()
                stats.append(
                    FileLoadStat(
                        file=filepath.replace(settings.DAGS_FOLDER, ""),
                        duration=file_parse_end_dttm - file_parse_start_dttm,
                        dag_num=len(result.dags),
                        task_num=sum(len(dag.tasks) for dag in result.dags.values()),
                        dags=str([dag.dag_id for dag in result.dags.values()]),
                        warning_num=len(result.import_warnings),
                    )
                )

            except Exception as e:
                self.log.exception(e)
        return DagsImportResult(
            # TODO: check collisions
            dags=functools.reduce(lambda agg, result: {**agg, **result.dags}, import_results, {}),
            import_errors=functools.reduce(lambda agg, result: {**agg, **result.import_errors}, import_results, {}),
            import_warnings=functools.reduce(lambda agg, result: {**agg, **result.import_errors}, import_results, {}),
        )

    def _process_file(self, filepath) -> DagsImportResult|None:
        """Given a path to a python module or zip file, import the module and look for dag objects within."""
        from airflow.models.dag import DagContext

        if filepath is None or not os.path.isfile(filepath):
            return None

        try:
            # TODO: put check here
            self.log.debug("Importing %s", filepath)
        except Exception as e:
            self.log.exception(e)
            return None

        # Ensure we don't pick up anything else we didn't mean to
        DagContext.autoregistered_dags.clear()

        with capture_with_reraise() as captured_warnings:
            if filepath.endswith(".py") or not zipfile.is_zipfile(filepath):
                mods, import_errors = self._load_modules_from_file(filepath)
            else:
                mods, import_errors = self._load_modules_from_zip(filepath)

        warnings = {}
        if captured_warnings:
            formatted_warnings = []
            for msg in captured_warnings:
                category = msg.category.__name__
                if (module := msg.category.__module__) != "builtins":
                    category = f"{module}.{category}"
                formatted_warnings.append(f"{msg.filename}:{msg.lineno}: {category}: {msg.message}")
            warnings = {filepath: tuple(formatted_warnings)}

        found_dags, module_import_errors = self._process_modules(filepath, mods)
        import_errors.update(module_import_errors)

        return DagsImportResult(
            dags={dag.dag_id: dag for dag in found_dags},
            import_warnings=warnings,
            import_errors=import_errors,
        )

    def _load_modules_from_file(self, filepath) -> Tuple[Collection[ModuleType], Mapping[str, str]]:
        from airflow.models.dag import DagContext

        if not might_contain_dag(filepath, self.safe_mode):
            # Don't want to spam user with skip messages
            
            self.log.info("File %s assumed to contain no DAGs. Skipping.", filepath)
            return [], None

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
                return [new_module], {}
            except (Exception, AirflowTaskTimeout) as e:
                DagContext.autoregistered_dags.clear()
                self.log.exception("Failed to import: %s", filepath)
                if self.dagbag_import_error_tracebacks:
                    return [], {
                        filepath: traceback.format_exc(
                            limit=-self.dagbag_import_error_traceback_depth)
                    }
                else:
                    return [], {filepath: str(e)}

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

    def _load_modules_from_zip(self, filepath) -> Tuple[Collection[ModuleType], Mapping[str, str]]:
        from airflow.models.dag import DagContext

        mods = []
        import_errors = {}
        with zipfile.ZipFile(filepath) as current_zip_file:
            for zip_info in current_zip_file.infolist():
                zip_path = pathlib.Path(zip_info.filename)
                if zip_path.suffix not in [".py", ".pyc"] or len(zip_path.parts) > 1:
                    continue

                if zip_path.stem == "__init__":
                    self.log.warning("Found %s at root of %s", zip_path.name, filepath)

                self.log.debug("Reading %s from %s", zip_info.filename, filepath)

                if not might_contain_dag(zip_info.filename, self.safe_mode, current_zip_file):
                    # todo: create ignore list
                    # Don't want to spam user with skip messages
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
                    if self.dagbag_import_error_tracebacks:
                        import_errors[fileloc] = traceback.format_exc(
                            limit=-self.dagbag_import_error_traceback_depth
                        )
                    else:
                        import_errors[fileloc] = str(e)
                finally:
                    if sys.path[0] == filepath:
                        del sys.path[0]
        return mods, import_errors

    def _process_modules(self, filepath, mods) -> Tuple[Collection[DAG], Mapping[str, str]]:
        from airflow.models.dag import DAG, DagContext  # Avoid circular import

        top_level_dags = {(o, m) for m in mods for o in m.__dict__.values() if isinstance(o, DAG)}

        top_level_dags.update(DagContext.autoregistered_dags)

        DagContext.current_autoregister_module_name = None
        DagContext.autoregistered_dags.clear()

        found_dags = []
        import_errors = {}

        for dag, mod in top_level_dags:
            dag.fileloc = mod.__file__
            try:
                self._validate_dag(dag=dag)
            except AirflowClusterPolicySkipDag:
                pass
            except Exception as e:
                self.log.exception("Failed to bag_dag: %s", dag.fileloc)
                import_errors[dag.fileloc] = f"{type(e).__name__}: {e}"
            else:
                found_dags.append(dag)
        return found_dags, import_errors

    def _validate_dag(self, dag: DAG):
        """
        Add the DAG into the bag.

        :raises: AirflowDagCycleException if a cycle is detected in this dag or its subdags.
        :raises: AirflowDagDuplicatedIdException if this dag or its subdags already exists in the bag.
        """

        dag.validate()
        check_cycle(dag)  # throws if a task cycle is found

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

        self.log.debug("Validated DAG %s", dag)

    def _list_py_file_paths(self, subdir: str) -> list[str]:
        # Ensure dag_folder is a str -- it may have been a pathlib.Path
        corrected_subdir = correct_maybe_zipped(subdir)
        return list_py_file_paths(
            corrected_subdir,
            safe_mode=self.safe_mode,
            include_examples=False,
        )