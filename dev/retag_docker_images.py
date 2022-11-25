#!/usr/bin/env python
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

import rich_click as click

PYTHON_VERSIONS = ["3.7", "3.8", "3.9", "3.10"]

GHCR_IO_PREFIX = "ghcr.io"


GHCR_IO_IMAGES = [
    "{prefix}/{repo}/{branch}/ci/python{python}:latest",
    "{prefix}/{repo}/{branch}/prod/python{python}:latest",
]


# noinspection StrFormat
def pull_push_all_images(
    source_prefix: str,
    target_prefix: str,
    images: list[str],
    source_branch: str,
    source_repo: str,
    target_branch: str,
    target_repo: str,
):
    for python in PYTHON_VERSIONS:
        for image in images:
            source_image = image.format(
                prefix=source_prefix, branch=source_branch, repo=source_repo, python=python
            )
            target_image = image.format(
                prefix=target_prefix, branch=target_branch, repo=target_repo, python=python
            )
            print(f"Copying image: {source_image} -> {target_image}")
            subprocess.run(
                ["regctl", "image", "copy", "--force-recursive", "--digest-tags", source_image, target_image],
                check=True,
            )


@click.group(invoke_without_command=True)
@click.option("--source-branch", type=str, default="main", help="Source branch name [main]")
@click.option("--target-branch", type=str, default="main", help="Target branch name [main]")
@click.option("--source-repo", type=str, default="apache/airflow", help="Source repo")
@click.option("--target-repo", type=str, default="apache/airflow", help="Target repo")
def main(
    source_branch: str,
    target_branch: str,
    source_repo: str,
    target_repo: str,
):
    pull_push_all_images(
        GHCR_IO_PREFIX, GHCR_IO_PREFIX, GHCR_IO_IMAGES, source_branch, source_repo, target_branch, target_repo
    )


if __name__ == "__main__":
    main()
