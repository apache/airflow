#!/usr/bin/python3
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
#
#
# This scripts re-tags images from one branch to another. Since we keep
# images "per-branch" we sometimes need to "clone" the current
# images to provide a starting cache image to build images in the
# new branch. This can be useful in a few situations:
#
# * when starting new release branch (for example `v2-1-test`)
# * when renaming a branch
#
import subprocess
from typing import List

import click

PYTHON_VERSIONS = ["3.6", "3.7", "3.8", "3.9"]

GHCR_IO_PREFIX = "ghcr.io/apache/airflow"

GHCR_IO_IMAGES = [
    "{prefix}/{branch}/ci-manifest/python{python_version}:latest",
    "{prefix}/{branch}/ci/python{python_version}:latest",
    "{prefix}/{branch}/prod-build/python{python_version}-build-v2:latest",
    "{prefix}/{branch}/prod/python{python_version}-build-v2:latest",
    "{prefix}/{branch}/python:{python_version}-slim-buster",
]


# noinspection StrFormat
def pull_push_all_images(
    source_prefix: str, target_prefix: str, images: List[str], source_branch: str, target_branch: str
):
    for python_version in PYTHON_VERSIONS:
        for image in images:
            source_image = image.format(
                prefix=source_prefix, branch=source_branch, python_version=python_version
            )
            target_image = image.format(
                prefix=target_prefix, branch=target_branch, python_version=python_version
            )
            print(f"Copying image: {source_image} -> {target_image}")
            subprocess.run(["docker", "pull", source_image], check=True)
            subprocess.run(["docker", "tag", source_image, target_image], check=True)
            subprocess.run(["docker", "push", target_image], check=True)


@click.group(invoke_without_command=True)
@click.option("--source-branch", type=str, default="main", help="Source branch name [main]")
@click.option("--target-branch", type=str, default="main", help="Target branch name [main]")
def main(
    source_branch: str,
    target_branch: str,
):
    pull_push_all_images(GHCR_IO_PREFIX, GHCR_IO_PREFIX, GHCR_IO_IMAGES, source_branch, target_branch)


if __name__ == "__main__":
    main()
