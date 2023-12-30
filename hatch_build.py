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

import logging
import os
from pathlib import Path
from subprocess import run
from typing import Any, Callable, Iterable

from hatchling.builders.config import BuilderConfig
from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from hatchling.builders.plugin.interface import BuilderInterface
from hatchling.plugin.manager import PluginManager

log = logging.getLogger(__name__)
log_level = logging.getLevelName(os.getenv("CUSTOM_AIRFLOW_BUILD_LOG_LEVEL", "INFO"))
log.setLevel(log_level)


class CustomBuild(BuilderInterface[BuilderConfig, PluginManager]):
    """Custom build class for Airflow assets and git version."""

    PLUGIN_NAME = "custom"

    def clean(self, directory: str, versions: Iterable[str]) -> None:
        work_dir = Path(self.root)
        commands = [
            ["rm -rf airflow/www/static/dist"],
            ["rm -rf airflow/www/node_modules"],
        ]
        for cmd in commands:
            run(cmd, cwd=work_dir.as_posix(), check=True, shell=True)

    def get_version_api(self) -> dict[str, Callable[..., str]]:
        """Custom build target for standard package preparation."""
        return {"standard": self.build_standard}

    def build_standard(self, directory: str, artifacts: Any, **build_data: Any) -> str:
        self.write_git_version()
        work_dir = Path(self.root)
        commands = [
            ["pre-commit run --hook-stage manual compile-www-assets --all-files"],
        ]
        for cmd in commands:
            run(cmd, cwd=work_dir.as_posix(), check=True, shell=True)
        dist_path = work_dir / "airflow" / "www" / "static" / "dist"
        return dist_path.resolve().as_posix()

    def get_git_version(self) -> str:
        """
        Return a version to identify the state of the underlying git repo.

        The version will indicate whether the head of the current git-backed working directory
        is tied to a release tag or not. It will indicate the former with a 'release:{version}'
        prefix and the latter with a '.dev0' suffix. Following the prefix will be a sha of the
        current branch head. Finally, a "dirty" suffix is appended to indicate that uncommitted
        changes are present.

        Example pre-release version: ".dev0+2f635dc265e78db6708f59f68e8009abb92c1e65".
        Example release version: ".release+2f635dc265e78db6708f59f68e8009abb92c1e65".
        Example modified release version: ".release+2f635dc265e78db6708f59f68e8009abb92c1e65".dirty

        :return: Found Airflow version in Git repo.
        """
        try:
            import git

            try:
                repo = git.Repo(str(Path(self.root) / ".git"))
            except git.NoSuchPathError:
                log.warning(".git directory not found: Cannot compute the git version")
                return ""
            except git.InvalidGitRepositoryError:
                log.warning("Invalid .git directory not found: Cannot compute the git version")
                return ""
        except ImportError:
            log.warning("gitpython not found: Cannot compute the git version.")
            return ""
        if repo:
            sha = repo.head.commit.hexsha
            if repo.is_dirty():
                return f".dev0+{sha}.dirty"
            # commit is clean
            return f".release:{sha}"
        return "no_git_version"

    def write_git_version(self) -> None:
        """Write git version to git_version file."""
        version = self.get_git_version()
        git_version_file = Path(self.root) / "airflow" / "git_version"
        self.app.display(f"Writing version {version} to {git_version_file}")
        git_version_file.write_text(version)


# List of pre-installed providers that are dynamically added to generated standard wheel packages
# That are released in PyPI. Those packages are not present in pyproject.toml as dependencies, and
# they are not installed when you install Airflow for editable installation for development.
# This way, when you develop Airflow you can work on Airflow and Providers together from the same
# Source tree - without polluting your editable installation with installed provider packages.
PREINSTALLED_PROVIDERS = [
    "apache-airflow-providers-http",
    "apache-airflow-providers-common-io",
    "apache-airflow-providers-common-sql",
    "apache-airflow-providers-ftp",
    "apache-airflow-providers-http",
    "apache-airflow-providers-imap",
    "apache-airflow-providers-sqlite",
]


class CustomBuildHook(BuildHookInterface[BuilderConfig]):
    """Custom build hook for Airflow - remove devel extras and adds preinstalled providers."""

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        """
        This occurs immediately before each build.

        Any modifications to the build data will be seen by the build target.
        """
        if version == "standard":
            # remove devel dependencies from optional dependencies for standard packages
            self.metadata.core._optional_dependencies = {
                key: value
                for (key, value) in self.metadata.core.optional_dependencies.items()
                if not key.startswith("devel") and key not in ["doc", "doc_gen"]
            }
            # Inject preinstalled providers into the dependencies for standard packages
            for provider in PREINSTALLED_PROVIDERS:
                self.metadata.core._dependencies.append(provider)
