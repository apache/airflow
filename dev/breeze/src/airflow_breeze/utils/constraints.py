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

import multiprocessing as mp
from typing import List, Tuple

from airflow_breeze.shell.shell_params import ShellParams
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.docker_command_utils import (
    construct_env_variables_docker_compose_command,
    get_extra_docker_flags,
)
from airflow_breeze.utils.parallel import check_async_run_results
from airflow_breeze.utils.run_utils import run_command


def run_generate_constraints(
    shell_params: ShellParams, dry_run: bool, verbose: bool, generate_constraints_mode: str
) -> Tuple[int, str]:
    env_variables = construct_env_variables_docker_compose_command(shell_params)
    extra_docker_flags = get_extra_docker_flags(shell_params.mount_sources)
    cmd_to_run = [
        "docker",
        "run",
        "-t",
        *extra_docker_flags,
        "-e",
        "SKIP_ENVIRONMENT_INITIALIZATION=true",
        "-e",
        f"GENERATE_CONSTRAINTS_MODE={generate_constraints_mode}",
        "--pull",
        "never",
        shell_params.airflow_image_name_with_tag,
        "/opt/airflow/scripts/in_container/run_generate_constraints.sh",
    ]
    generate_constraints_result = run_command(cmd_to_run, verbose=verbose, dry_run=dry_run, env=env_variables)
    return (
        generate_constraints_result.returncode,
        f"Generate constraints Python {shell_params.python}:{generate_constraints_mode}",
    )


def run_generate_constraints_in_parallel(
    shell_params_list: List[ShellParams],
    python_version_list: List[str],
    generate_constraints_mode: str,
    parallelism: int,
    dry_run: bool,
    verbose: bool,
):
    """Run generate constraints in parallel"""
    get_console().print(
        f"\n[info]Generating constraints with parallelism = {parallelism} "
        f"for the constraints: {python_version_list}[/]"
    )
    pool = mp.Pool(parallelism)
    results = [
        pool.apply_async(
            run_generate_constraints,
            args=(shell_param, dry_run, verbose, generate_constraints_mode),
        )
        for shell_param in shell_params_list
    ]
    check_async_run_results(results)
    pool.close()
