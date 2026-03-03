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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.spark.hooks.spark_pipelines import SparkPipelinesHook
from airflow.providers.common.compat.openlineage.utils.spark import (
    inject_parent_job_information_into_spark_properties,
    inject_transport_information_into_spark_properties,
)
from airflow.providers.common.compat.sdk import BaseOperator, conf

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class SparkPipelinesOperator(BaseOperator):
    """
    Execute Spark Declarative Pipelines using the spark-pipelines CLI.

    This operator wraps the spark-pipelines binary to execute declarative data pipelines.
    It supports running pipelines, dry-runs for validation, and initializing new pipeline projects.

    .. seealso::
        For more information on Spark Declarative Pipelines, see the guide:
        https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html

    :param pipeline_spec: Path to the pipeline specification file (YAML). (templated)
    :param pipeline_command: The spark-pipelines command to execute ('run', 'dry-run'). Default is 'run'.
    :param conf: Arbitrary Spark configuration properties (templated)
    :param conn_id: The :ref:`spark connection id <howto/connection:spark-submit>` as configured
        in Airflow administration. When an invalid connection_id is supplied, it will default to yarn.
    :param num_executors: Number of executors to launch
    :param executor_cores: Number of cores per executor (Default: 2)
    :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
    :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)
    :param verbose: Whether to pass the verbose flag to spark-pipelines process for debugging
    :param env_vars: Environment variables for spark-pipelines. (templated)
    :param deploy_mode: Whether to deploy your driver on the worker nodes (cluster) or locally as a client.
    :param yarn_queue: The name of the YARN queue to which the application is submitted.
    :param keytab: Full path to the file that contains the keytab (templated)
    :param principal: The name of the kerberos principal used for keytab (templated)
    :param openlineage_inject_parent_job_info: Whether to inject OpenLineage parent job information
    :param openlineage_inject_transport_info: Whether to inject OpenLineage transport information
    """

    template_fields: Sequence[str] = (
        "pipeline_spec",
        "conf",
        "env_vars",
        "keytab",
        "principal",
    )

    def __init__(
        self,
        *,
        pipeline_spec: str | None = None,
        pipeline_command: str = "run",
        conf: dict[Any, Any] | None = None,
        conn_id: str = "spark_default",
        num_executors: int | None = None,
        executor_cores: int | None = None,
        executor_memory: str | None = None,
        driver_memory: str | None = None,
        verbose: bool = False,
        env_vars: dict[str, Any] | None = None,
        deploy_mode: str | None = None,
        yarn_queue: str | None = None,
        keytab: str | None = None,
        principal: str | None = None,
        openlineage_inject_parent_job_info: bool = conf.getboolean(
            "openlineage", "spark_inject_parent_job_info", fallback=False
        ),
        openlineage_inject_transport_info: bool = conf.getboolean(
            "openlineage", "spark_inject_transport_info", fallback=False
        ),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.pipeline_spec = pipeline_spec
        self.pipeline_command = pipeline_command
        self.conf = conf
        self.num_executors = num_executors
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory
        self.verbose = verbose
        self.env_vars = env_vars
        self.deploy_mode = deploy_mode
        self.yarn_queue = yarn_queue
        self.keytab = keytab
        self.principal = principal
        self._conn_id = conn_id
        self._openlineage_inject_parent_job_info = openlineage_inject_parent_job_info
        self._openlineage_inject_transport_info = openlineage_inject_transport_info

    def execute(self, context: Context) -> None:
        """Execute the SparkPipelinesHook to run the specified pipeline command."""
        self.conf = self.conf or {}
        if self._openlineage_inject_parent_job_info:
            self.log.debug("Injecting OpenLineage parent job information into Spark properties.")
            self.conf = inject_parent_job_information_into_spark_properties(self.conf, context)
        if self._openlineage_inject_transport_info:
            self.log.debug("Injecting OpenLineage transport information into Spark properties.")
            self.conf = inject_transport_information_into_spark_properties(self.conf, context)

        self.hook.submit_pipeline()

    def on_kill(self) -> None:
        self.hook.on_kill()

    @cached_property
    def hook(self) -> SparkPipelinesHook:
        return SparkPipelinesHook(
            pipeline_spec=self.pipeline_spec,
            pipeline_command=self.pipeline_command,
            conf=self.conf,
            conn_id=self._conn_id,
            num_executors=self.num_executors,
            executor_cores=self.executor_cores,
            executor_memory=self.executor_memory,
            driver_memory=self.driver_memory,
            verbose=self.verbose,
            env_vars=self.env_vars,
            deploy_mode=self.deploy_mode,
            yarn_queue=self.yarn_queue,
            keytab=self.keytab,
            principal=self.principal,
        )
