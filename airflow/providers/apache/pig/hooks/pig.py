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

import subprocess
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class PigCliHook(BaseHook):
    """Simple wrapper around the pig CLI.

    :param pig_cli_conn_id: Connection id used by the hook
    :param pig_properties: additional properties added after pig cli command as list of strings.
    """

    conn_name_attr = "pig_cli_conn_id"
    default_conn_name = "pig_cli_default"
    conn_type = "pig_cli"
    hook_name = "Pig Client Wrapper"

    def __init__(
        self, pig_cli_conn_id: str = default_conn_name, pig_properties: list[str] | None = None
    ) -> None:
        super().__init__()
        conn = self.get_connection(pig_cli_conn_id)
        conn_pig_properties = conn.extra_dejson.get("pig_properties")
        if conn_pig_properties:
            raise RuntimeError(
                "The PigCliHook used to have possibility of passing `pig_properties` to the Hook,"
                " however with the 4.0.0 version of `apache-pig` provider it has been removed. You should"
                " use ``pig_opts`` (space separated string) or ``pig_properties`` (string list) in the"
                " PigOperator. You can also pass ``pig-properties`` in the PigCliHook `init`. Currently,"
                f" the {pig_cli_conn_id} connection has those extras: `{conn_pig_properties}`."
            )
        self.pig_properties = pig_properties or []
        self.conn = conn
        self.sub_process = None

    def run_cli(self, pig: str, pig_opts: str | None = None, verbose: bool = True) -> Any:
        """
        Run a pig script using the pig cli.

        >>> ph = PigCliHook()
        >>> result = ph.run_cli("ls /;", pig_opts="-x mapreduce")
        >>> ("hdfs://" in result)
        True
        """
        with TemporaryDirectory(prefix="airflow_pigop_") as tmp_dir, NamedTemporaryFile(dir=tmp_dir) as f:
            f.write(pig.encode("utf-8"))
            f.flush()
            fname = f.name
            pig_bin = "pig"
            cmd_extra: list[str] = []

            pig_cmd = [pig_bin]

            if self.pig_properties:
                pig_cmd.extend(self.pig_properties)
            if pig_opts:
                pig_opts_list = pig_opts.split()
                pig_cmd.extend(pig_opts_list)

            pig_cmd.extend(["-f", fname, *cmd_extra])

            if verbose:
                self.log.info("%s", " ".join(pig_cmd))
            sub_process: Any = subprocess.Popen(
                pig_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=tmp_dir, close_fds=True
            )
            self.sub_process = sub_process
            stdout = ""
            for line in iter(sub_process.stdout.readline, b""):
                stdout += line.decode("utf-8")
                if verbose:
                    self.log.info(line.strip())
            sub_process.wait()

            if sub_process.returncode:
                raise AirflowException(stdout)

            return stdout

    def kill(self) -> None:
        """Kill Pig job."""
        if self.sub_process:
            if self.sub_process.poll() is None:
                self.log.info("Killing the Pig job")
                self.sub_process.kill()
