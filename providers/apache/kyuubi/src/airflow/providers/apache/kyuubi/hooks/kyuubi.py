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

from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any

from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
from flask_babel import lazy_gettext
from wtforms import StringField

from airflow.exceptions import AirflowException
from airflow.providers.apache.hive.hooks.hive import HiveCliHook, get_context_from_env_var
import subprocess


class KyuubiHook(HiveCliHook):
    """
    Wrapper around the Kyuubi connection.

    Inherits from HiveCliHook to leverage Hive CLI/Beeline functionality,
    but allows specifying a custom Beeline path for Kyuubi.

    :param kyuubi_conn_id: Reference to the Kyuubi connection id.
    """

    conn_name_attr = "kyuubi_conn_id"
    default_conn_name = "kyuubi_default"
    conn_type = "kyuubi"
    hook_name = "Kyuubi"

    def __init__(
        self,
        kyuubi_conn_id: str = default_conn_name,
        spark_queue: str | None = None,
        spark_app_name: str | None = None,
        spark_sql_shuffle_partitions: str | None = None,
        spark_conf: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            hive_cli_conn_id=kyuubi_conn_id,
            mapred_queue=spark_queue,
            mapred_job_name=spark_app_name,
            **kwargs,
        )
        self.kyuubi_conn_id = kyuubi_conn_id
        self.spark_sql_shuffle_partitions = spark_sql_shuffle_partitions
        self.spark_conf = spark_conf or {}

    def run_cli(
        self,
        hql: str,
        schema: str | None = None,
        verbose: bool = True,
        hive_conf: dict[Any, Any] | None = None,
    ) -> Any:
        """
        Execute the hql using the hive cli (via beeline) for Kyuubi.

        :param hql: hql to be executed.
        :param schema: hive schema to use.
        :param verbose: whether to print verbose logs.
        :param hive_conf: hive_conf to execute alone with the hql.
        """
        conn = self.conn
        schema = schema or conn.schema or ""

        if schema:
            hql = f"USE {schema};\n{hql}"

        with TemporaryDirectory(prefix="airflow_kyuubi_") as tmp_dir, NamedTemporaryFile(dir=tmp_dir) as f:
            hql += "\n"
            f.write(hql.encode("UTF-8"))
            f.flush()
            hive_cmd = self._prepare_cli_cmd()
            env_context = get_context_from_env_var()
            if hive_conf:
                env_context.update(hive_conf)

            # Spark SQL configs
            spark_conf_params = self._prepare_hiveconf(env_context)
            if self.mapred_queue:
                spark_conf_params.extend(["--hiveconf", f"spark.yarn.queue={self.mapred_queue}"])
            if self.mapred_job_name:
                spark_conf_params.extend(["--hiveconf", f"spark.app.name={self.mapred_job_name}"])
            if self.spark_sql_shuffle_partitions:
                spark_conf_params.extend(
                    ["--hiveconf", f"spark.sql.shuffle.partitions={self.spark_sql_shuffle_partitions}"]
                )

            # User provided Spark configs
            for key, value in self.spark_conf.items():
                spark_conf_params.extend(["--hiveconf", f"{key}={value}"])

            # Error detection patterns
            kyuubi_error = "error='Cannot allocate memory'"
            jdbc_error = "Error: Could not open client transport with JDBC Uri:"
            custom_err_code = "ERROR111"
            err_content_list = [kyuubi_error, jdbc_error, custom_err_code]

            hive_cmd.extend(spark_conf_params)
            hive_cmd.extend(["-f", f.name])
            hive_cmd.extend(
                [
                    f"; if [ $? -ne 0 ];then echo {custom_err_code} ;exit 111; fi ; "
                ]
            )

            def __contains__(content_list, total_content):
                for content in content_list:
                    if content in total_content:
                        self.log.error("Detected error content: %s", content)
                        return True
                return False

            if verbose:
                self.log.info("Executing command: %s", " ".join(hive_cmd))

            sub_process = subprocess.Popen(
                hive_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=tmp_dir, close_fds=True
            )
            self.sub_process = sub_process
            stdout = ""
            while True:
                if sub_process.stdout:
                    line = sub_process.stdout.readline()
                    if not line:
                        break
                    line_str = line.decode("UTF-8")
                    stdout += line_str
                    if verbose:
                        self.log.info(line_str.strip())

                    if __contains__(err_content_list, line_str):
                        self.log.error("Detected Kyuubi-Spark session timeout or memory overflow error")
                        raise AirflowException(f"Execution failed, detected error: {line_str}")
                else:
                    break

            sub_process.wait()

            if sub_process.returncode:
                self.log.error("Command exited with non-zero code: %s", sub_process.returncode)
                raise AirflowException(
                    f"Command execution failed, return code: {sub_process.returncode}\nOutput: {stdout}"
                )

            self.log.info("Successfully executed using Kyuubi connection %s", self.kyuubi_conn_id)
            return stdout

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to Kyuubi connection form."""
        widgets = super().get_connection_form_widgets()
        widgets["beeline_path"] = StringField(
            lazy_gettext("Beeline Path"),  # type: ignore[arg-type]
            widget=BS3TextFieldWidget(),  # type: ignore[arg-type]
            description="Path to the Kyuubi Beeline executable (e.g., /path/to/bin/beeline)",
        )
        return widgets

    def _prepare_cli_cmd(self) -> list[Any]:
        """Create the command list from available information, using custom beeline path if provided."""
        cmd = super()._prepare_cli_cmd()
        
        # If use_beeline is enabled (which is default in HiveCliHook widgets, but check logic)
        # HiveCliHook sets defaults.
        
        beeline_path = self.conn.extra_dejson.get("beeline_path")
        if self.use_beeline and beeline_path:
            self.log.info("Using custom Beeline path: %s", beeline_path)
            cmd[0] = beeline_path
            
        return cmd
