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

from typing import Any, AsyncIterator

from airflow.providers.apache.beam.hooks.beam import BeamAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class BeamPipelineTrigger(BaseTrigger):
    """
    Trigger to perform checking the pipeline status until it reaches terminate state.

    :param variables: Variables passed to the pipeline.
    :param py_file: Path to the python file to execute.
    :param py_options: Additional options.
    :param py_interpreter: Python version of the Apache Beam pipeline. If `None`, this defaults to the
        python3. To track python versions supported by beam and related issues
        check: https://issues.apache.org/jira/browse/BEAM-1251
    :param py_requirements: Additional python package(s) to install.
        If a value is passed to this parameter, a new virtual environment has been created with
        additional packages installed.

        You could also install the apache-beam package if it is not installed on your system, or you want
        to use a different version.
    :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
        See virtualenv documentation for more information.

        This option is only relevant if the ``py_requirements`` parameter is not None.
    :param runner: Runner on which pipeline will be run. By default, "DirectRunner" is being used.
        Other possible options: DataflowRunner, SparkRunner, FlinkRunner, PortableRunner.
        See: :class:`~providers.apache.beam.hooks.beam.BeamRunnerType`
        See: https://beam.apache.org/documentation/runners/capability-matrix/
    """

    def __init__(
        self,
        variables: dict,
        py_file: str,
        py_options: list[str] | None = None,
        py_interpreter: str = "python3",
        py_requirements: list[str] | None = None,
        py_system_site_packages: bool = False,
        runner: str = "DirectRunner",
    ):
        super().__init__()
        self.variables = variables
        self.py_file = py_file
        self.py_options = py_options
        self.py_interpreter = py_interpreter
        self.py_requirements = py_requirements
        self.py_system_site_packages = py_system_site_packages
        self.runner = runner

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize BeamPipelineTrigger arguments and classpath."""
        return (
            "airflow.providers.apache.beam.triggers.beam.BeamPipelineTrigger",
            {
                "variables": self.variables,
                "py_file": self.py_file,
                "py_options": self.py_options,
                "py_interpreter": self.py_interpreter,
                "py_requirements": self.py_requirements,
                "py_system_site_packages": self.py_system_site_packages,
                "runner": self.runner,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Get current pipeline status and yields a TriggerEvent."""
        hook = self._get_async_hook()
        try:
            return_code = await hook.start_python_pipeline_async(
                variables=self.variables,
                py_file=self.py_file,
                py_options=self.py_options,
                py_interpreter=self.py_interpreter,
                py_requirements=self.py_requirements,
                py_system_site_packages=self.py_system_site_packages,
            )
        except Exception as e:
            self.log.exception("Exception occurred while checking for pipeline state")
            yield TriggerEvent({"status": "error", "message": str(e)})
        else:
            if return_code == 0:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": "Pipeline has finished SUCCESSFULLY",
                    }
                )
            else:
                yield TriggerEvent({"status": "error", "message": "Operation failed"})
        return

    def _get_async_hook(self) -> BeamAsyncHook:
        return BeamAsyncHook(runner=self.runner)
