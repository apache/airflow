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

from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
from airflow.settings import WEB_COLORS

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SparkSubmitOperator(BaseOperator):
    """
    Wrap the spark-submit binary to kick off a spark-submit job; requires "spark-submit" binary in the PATH.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SparkSubmitOperator`

    :param application: The application that submitted as a job, either jar or py file. (templated)
    :param conf: Arbitrary Spark configuration properties (templated)
    :param conn_id: The :ref:`spark connection id <howto/connection:spark-submit>` as configured
        in Airflow administration. When an invalid connection_id is supplied, it will default to yarn.
    :param files: Upload additional files to the executor running the job, separated by a
                  comma. Files will be placed in the working directory of each executor.
                  For example, serialized objects. (templated)
    :param py_files: Additional python files used by the job, can be .zip, .egg or .py. (templated)
    :param jars: Submit additional jars to upload and place them in executor classpath. (templated)
    :param driver_class_path: Additional, driver-specific, classpath settings. (templated)
    :param java_class: the main class of the Java application
    :param packages: Comma-separated list of maven coordinates of jars to include on the
                     driver and executor classpaths. (templated)
    :param exclude_packages: Comma-separated list of maven coordinates of jars to exclude
                             while resolving the dependencies provided in 'packages' (templated)
    :param repositories: Comma-separated list of additional remote repositories to search
                         for the maven coordinates given with 'packages'
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
                                 (Default: all the available cores on the worker)
    :param executor_cores: (Standalone & YARN only) Number of cores per executor (Default: 2)
    :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
    :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)
    :param keytab: Full path to the file that contains the keytab (templated)
                        (will overwrite any keytab defined in the connection's extra JSON)
    :param principal: The name of the kerberos principal used for keytab (templated)
                        (will overwrite any principal defined in the connection's extra JSON)
    :param proxy_user: User to impersonate when submitting the application (templated)
    :param name: Name of the job (default airflow-spark). (templated)
    :param num_executors: Number of executors to launch
    :param status_poll_interval: Seconds to wait between polls of driver status in cluster
        mode (Default: 1)
    :param application_args: Arguments for the application being submitted (templated)
    :param env_vars: Environment variables for spark-submit. It supports yarn and k8s mode too. (templated)
    :param verbose: Whether to pass the verbose flag to spark-submit process for debugging
    :param spark_binary: The command to use for spark submit.
                         Some distros may use spark2-submit or spark3-submit.
                         (will overwrite any spark_binary defined in the connection's extra JSON)
    :param properties_file: Path to a file from which to load extra properties. If not
                              specified, this will look for conf/spark-defaults.conf.
    :param yarn_queue: The name of the YARN queue to which the application is submitted.
                        (will overwrite any yarn queue defined in the connection's extra JSON)
    :param deploy_mode: Whether to deploy your driver on the worker nodes (cluster) or locally as a client.
                        (will overwrite any deployment mode defined in the connection's extra JSON)
    :param use_krb5ccache: if True, configure spark to use ticket cache instead of relying
                           on keytab for Kerberos login
    """

    template_fields: Sequence[str] = (
        "application",
        "conf",
        "files",
        "py_files",
        "jars",
        "driver_class_path",
        "packages",
        "exclude_packages",
        "keytab",
        "principal",
        "proxy_user",
        "name",
        "application_args",
        "env_vars",
        "properties_file",
    )
    ui_color = WEB_COLORS["LIGHTORANGE"]

    def __init__(
        self,
        *,
        application: str = "",
        conf: dict[str, Any] | None = None,
        conn_id: str = "spark_default",
        files: str | None = None,
        py_files: str | None = None,
        archives: str | None = None,
        driver_class_path: str | None = None,
        jars: str | None = None,
        java_class: str | None = None,
        packages: str | None = None,
        exclude_packages: str | None = None,
        repositories: str | None = None,
        total_executor_cores: int | None = None,
        executor_cores: int | None = None,
        executor_memory: str | None = None,
        driver_memory: str | None = None,
        keytab: str | None = None,
        principal: str | None = None,
        proxy_user: str | None = None,
        name: str = "arrow-spark",
        num_executors: int | None = None,
        status_poll_interval: int = 1,
        application_args: list[Any] | None = None,
        env_vars: dict[str, Any] | None = None,
        verbose: bool = False,
        spark_binary: str | None = None,
        properties_file: str | None = None,
        yarn_queue: str | None = None,
        deploy_mode: str | None = None,
        use_krb5ccache: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.application = application
        self.conf = conf
        self.files = files
        self.py_files = py_files
        self._archives = archives
        self.driver_class_path = driver_class_path
        self.jars = jars
        self._java_class = java_class
        self.packages = packages
        self.exclude_packages = exclude_packages
        self._repositories = repositories
        self._total_executor_cores = total_executor_cores
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self.keytab = keytab
        self.principal = principal
        self.proxy_user = proxy_user
        self.name = name
        self._num_executors = num_executors
        self._status_poll_interval = status_poll_interval
        self.application_args = application_args
        self.env_vars = env_vars
        self._verbose = verbose
        self._spark_binary = spark_binary
        self.properties_file = properties_file
        self._yarn_queue = yarn_queue
        self._deploy_mode = deploy_mode
        self._hook: SparkSubmitHook | None = None
        self._conn_id = conn_id
        self._use_krb5ccache = use_krb5ccache

    def execute(self, context: Context) -> None:
        """Call the SparkSubmitHook to run the provided spark job."""
        if self._hook is None:
            self._hook = self._get_hook()
        self._hook.submit(self.application)

    def on_kill(self) -> None:
        if self._hook is None:
            self._hook = self._get_hook()
        self._hook.on_kill()

    def _get_hook(self) -> SparkSubmitHook:
        return SparkSubmitHook(
            conf=self.conf,
            conn_id=self._conn_id,
            files=self.files,
            py_files=self.py_files,
            archives=self._archives,
            driver_class_path=self.driver_class_path,
            jars=self.jars,
            java_class=self._java_class,
            packages=self.packages,
            exclude_packages=self.exclude_packages,
            repositories=self._repositories,
            total_executor_cores=self._total_executor_cores,
            executor_cores=self._executor_cores,
            executor_memory=self._executor_memory,
            driver_memory=self._driver_memory,
            keytab=self.keytab,
            principal=self.principal,
            proxy_user=self.proxy_user,
            name=self.name,
            num_executors=self._num_executors,
            status_poll_interval=self._status_poll_interval,
            application_args=self.application_args,
            env_vars=self.env_vars,
            verbose=self._verbose,
            spark_binary=self._spark_binary,
            properties_file=self.properties_file,
            yarn_queue=self._yarn_queue,
            deploy_mode=self._deploy_mode,
            use_krb5ccache=self._use_krb5ccache,
        )
