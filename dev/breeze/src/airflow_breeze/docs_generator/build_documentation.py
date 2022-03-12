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
from airflow_breeze.docs_generator.doc_builder import DocBuilder
from airflow_breeze.global_constants import MOUNT_ALL_LOCAL_SOURCES, MOUNT_SELECTED_LOCAL_SOURCES
from airflow_breeze.utils.docker_command_utils import get_extra_docker_flags
from airflow_breeze.utils.run_utils import run_command


def build(
    verbose: bool,
    airflow_sources: str,
    airflow_ci_image_name: str,
    doc_builder: DocBuilder,
):
    extra_docker_flags = get_extra_docker_flags(
        MOUNT_ALL_LOCAL_SOURCES, MOUNT_SELECTED_LOCAL_SOURCES, airflow_sources
    )
    cmd = []
    cmd.extend(["docker", "run"])
    cmd.extend(extra_docker_flags)
    cmd.extend(["-t", "-e", "GITHUB_ACTIONS="])
    cmd.extend(["--entrypoint", "/usr/local/bin/dumb-init", "--pull", "never"])
    cmd.extend([airflow_ci_image_name, "--", "/opt/airflow/scripts/in_container/run_docs_build.sh"])
    cmd.extend(doc_builder.args_doc_builder)
    run_command(cmd, verbose=verbose, text=True)
