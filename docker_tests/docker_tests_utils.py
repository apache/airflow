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

from docker_tests.command_utils import run_command

docker_image = os.environ.get("DOCKER_IMAGE")

if not docker_image:
    raise Exception("The DOCKER_IMAGE environment variable is required")


def run_bash_in_docker(bash_script, **kwargs):
    docker_command = [
        "docker",
        "run",
        "--rm",
        "-e",
        "COLUMNS=180",
        "--entrypoint",
        "/bin/bash",
        docker_image,
        "-c",
        bash_script,
    ]
    return run_command(docker_command, **kwargs)


def run_python_in_docker(python_script, **kwargs):
    docker_command = [
        "docker",
        "run",
        "--rm",
        "-e",
        "COLUMNS=180",
        "-e",
        "PYTHONDONTWRITEBYTECODE=true",
        docker_image,
        "python",
        "-c",
        python_script,
    ]
    return run_command(docker_command, **kwargs)


def display_dependency_conflict_message():
    print(
        """
***** Beginning of the instructions ****

The image did not pass 'pip check' verification. This means that there are some conflicting dependencies
in the image.

It can mean one of those:

1) The main is currently broken (other PRs will fail with the same error)
2) You changed some dependencies in setup.py or setup.cfg and they are conflicting.



In case 1) - apologies for the trouble.Please let committers know and they will fix it. You might
be asked to rebase to the latest main after the problem is fixed.

In case 2) - Follow the steps below:

* try to build CI and then PROD image locally with breeze, adding --upgrade-to-newer-dependencies flag
  (repeat it for all python versions)

CI image:

     breeze ci-image build --upgrade-to-newer-dependencies --python 3.7

Production image:

     breeze ci-image build --production-image --upgrade-to-newer-dependencies --python 3.7

* You will see error messages there telling which requirements are conflicting and which packages caused the
  conflict. Add the limitation that caused the conflict to EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS
  variable in Dockerfile.ci. Note that the limitations might be different for Dockerfile.ci and Dockerfile
  because not all packages are installed by default in the PROD Dockerfile. So you might find that you
  only need to add the limitation to the Dockerfile.ci

***** End of the instructions ****
"""
    )
