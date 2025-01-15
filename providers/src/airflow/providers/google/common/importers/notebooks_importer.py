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

import functools
import pathlib
import os
import sys
import types
from typing import Iterable, TYPE_CHECKING

import nbformat
import IPython

from airflow.dag_processing.dag_importer import DagImporter, DagsImportResult, ImportOptions, PathData
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
from airflow import settings
from airflow.configuration import conf
from airflow.utils import timezone
from airflow.utils.dag_cycle_tester import check_cycle


def _list_ipynb_files(dagpath: str) -> list[str]:
    ipynb_files = []
    if pathlib.Path(dagpath).is_dir():
        for filename in os.listdir(dagpath):
            if filename.endswith(".ipynb"):
                ipynb_files.append(os.path.join(dagpath, filename))
    else:
        ipynb_files.append(dagpath)
    return ipynb_files



class NotebooksImporter(DagImporter):
    """
    This is just a simplified demo, it is not going to be included as part of AIP-85.
    """
    def import_path(self, dagpath: str, options: ImportOptions | None = None) -> Generator[DagsImportResult, None, None]:
        from airflow.models.dag import DagContext

        ipynb_files = _list_ipynb_files(dagpath)
        import_results = []
        for notebook in ipynb_files:
            try:
                module_name = pathlib.Path(notebook).name
                with open(notebook, "r") as f:
                    content = nbformat.read(f, 4)
                
                mod = types.ModuleType(module_name)
                mod.__loader__ = self
                mod.__dict__["get_ipython"] = IPython.get_ipython
                mod.__file__ = notebook
                sys.modules[module_name] = mod
                ipython_instance = IPython.core.interactiveshell.InteractiveShell.instance()
                instance_ns = ipython_instance.user_ns
                ipython_instance.user_ns = mod.__dict__

                DagContext.current_autoregister_module_name = module_name
                try:
                    for cell in content.cells:
                        if cell.cell_type != "code":
                            continue
                        rendered_cell = ipython_instance.input_transformer_manager.transform_cell(cell.source)
                        exec(rendered_cell, mod.__dict__)
                except (Exception, AirflowTaskTimeout) as e:
                    DagContext.autoregistered_dags.clear()
                    self.log.exception("Failed to import: %s", notebook)
                    import_results.append(DagsImportResult(
                        dags={},
                        import_warnings={},
                        import_errors={notebook: str(e)},
                    ))
                finally:
                    ipython_instance.user_ns = instance_ns
                found_dags, module_import_errors = self._process_modules(notebook, [mod])

                import_results.append(DagsImportResult(
                    dags={dag.dag_id: dag for dag in found_dags},
                    import_warnings={},
                    import_errors=module_import_errors,
                ))

            except Exception as e:
                self.log.exception(e)
        
        yield DagsImportResult(
            # TODO: check collisions
            dags=functools.reduce(lambda agg, result: {**agg, **result.dags}, import_results, {}),
            import_errors=functools.reduce(lambda agg, result: {**agg, **result.import_errors}, import_results, {}),
            import_warnings=functools.reduce(lambda agg, result: {**agg, **result.import_errors}, import_results, {}),
        )

    def list_paths(self, subpath: str) -> Iterable[PathData]:
        return map(lambda path: PathData(path), _list_ipynb_files(subpath))

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
