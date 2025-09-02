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

import os

from python_on_whales import docker

from docker_tests.constants import DEFAULT_DOCKER_IMAGE


def run_cmd_in_docker(
    cmd: list[str] | None = None,
    image: str | None = None,
    entrypoint: str | None = None,
    envs: dict[str, str] | None = None,
    remove: bool = True,
    **kwargs,
):
    cmd = cmd or []
    envs = envs or {}
    return docker.run(
        image=image or os.environ.get("DOCKER_IMAGE") or DEFAULT_DOCKER_IMAGE,
        entrypoint=entrypoint,
        command=cmd,
        remove=remove,
        envs={"COLUMNS": "180", **envs},
        **kwargs,
    )


def run_bash_in_docker(bash_script: str, **kwargs):
    kwargs.pop("entrypoint", None)
    return run_cmd_in_docker(cmd=["-c", bash_script], entrypoint="/bin/bash", **kwargs)


def run_python_in_docker(python_script, **kwargs):
    kwargs.pop("entrypoint", None)
    envs = {"PYTHONDONTWRITEBYTECODE": "true", **kwargs.pop("envs", {})}
    return run_cmd_in_docker(cmd=["python", "-c", python_script], envs=envs, **kwargs)


def run_airflow_cmd_in_docker(cmd: list[str] | None = None, **kwargs):
    kwargs.pop("entrypoint", None)
    return run_cmd_in_docker(cmd=["airflow", *(cmd or [])], **kwargs)


def display_dependency_conflict_message():
    print(
        """
***** Beginning of the instructions ****

The image did not pass 'pip check' verification. This means that there are some conflicting dependencies
in the image.

It can mean one of those:

1) The main is currently broken (other PRs will fail with the same error)
2) You changed some dependencies in pyproject.toml (either manually or automatically by prek)
   and they are conflicting.



In case 1) - apologies for the trouble.Please let committers know and they will fix it. You might
be asked to rebase to the latest main after the problem is fixed.

In case 2) - Follow the steps below:

* try to build CI and then PROD image locally with breeze, adding --upgrade-to-newer-dependencies flag
  (repeat it for all python versions)

CI image:

     breeze ci-image build --upgrade-to-newer-dependencies --python 3.10

Production image:

     breeze ci-image build --production-image --upgrade-to-newer-dependencies --python 3.10

***** End of the instructions ****
"""
    )
