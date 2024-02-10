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

import json
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

AIRFLOW_ROOT_PATH = Path(__file__).parent.parent.resolve()
GENERATED_PROVIDERS_DEPENDENCIES_FILE = AIRFLOW_ROOT_PATH / "generated" / "provider_dependencies.json"
DEV_DIR_PATH = AIRFLOW_ROOT_PATH / "dev"
PREINSTALLED_PROVIDERS_FILE = DEV_DIR_PATH / "airflow_pre_installed_providers.txt"
DEPENDENCIES = json.loads(GENERATED_PROVIDERS_DEPENDENCIES_FILE.read_text())
PREINSTALLED_PROVIDER_IDS = [
    package.strip()
    for package in PREINSTALLED_PROVIDERS_FILE.read_text().splitlines()
    if not package.strip().startswith("#")
]

# if providers are ready, we can preinstall them
PREINSTALLED_PROVIDERS = [
    f"apache-airflow-providers-{provider_id.replace('.','-')}"
    for provider_id in PREINSTALLED_PROVIDER_IDS
    if DEPENDENCIES[provider_id]["state"] == "ready"
]
# if provider is in not-ready or pre-release, we need to install its dependencies
# however we need to skip apache-airflow itself and potentially any providers that are
PREINSTALLED_NOT_READY_DEPS = []
for provider_id in PREINSTALLED_PROVIDER_IDS:
    if DEPENDENCIES[provider_id]["state"] not in ["ready", "suspended", "removed"]:
        for dependency in DEPENDENCIES[provider_id]["deps"]:
            if dependency.startswith("apache-airflow-providers"):
                raise Exception(
                    f"The provider {provider_id} is pre-installed and it has as dependency "
                    f"to another provider {dependency}. This is not allowed. Pre-installed"
                    f"providers should only have 'apache-airflow' and regular dependencies."
                )
            if not dependency.startswith("apache-airflow"):
                PREINSTALLED_NOT_READY_DEPS.append(dependency)


class CustomBuild(BuilderInterface[BuilderConfig, PluginManager]):
    """Custom build class for Airflow assets and git version."""

    # Note that this name of the plugin MUST be `custom` - as long as we use it from custom
    # hatch_build.py file and not from external plugin. See note in the:
    # https://hatch.pypa.io/latest/plugins/build-hook/custom/#example
    #
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
                if not key.startswith("devel") and key not in ["doc", "doc-gen"]
            }
            # Replace editable dependencies with provider dependencies for provider packages
            for dependency_id in DEPENDENCIES.keys():
                if DEPENDENCIES[dependency_id]["state"] != "ready":
                    continue
                normalized_dependency_id = dependency_id.replace(".", "-")
                self.metadata.core._optional_dependencies[normalized_dependency_id] = [
                    f"apache-airflow-providers-{normalized_dependency_id}"
                ]
            # Inject preinstalled providers into the dependencies for standard packages
            if self.metadata.core._dependencies:
                for provider in PREINSTALLED_PROVIDERS:
                    self.metadata.core._dependencies.append(provider)
                for dependency in PREINSTALLED_NOT_READY_DEPS:
                    self.metadata.core._dependencies.append(dependency)
