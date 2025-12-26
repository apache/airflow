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
import shutil
from collections.abc import Callable, Iterable
from pathlib import Path
from subprocess import run
from typing import Any

from hatchling.builders.config import BuilderConfig
from hatchling.builders.plugin.interface import BuilderInterface
from hatchling.plugin.manager import PluginManager

log = logging.getLogger(__name__)
log_level = logging.getLevelName(os.getenv("CUSTOM_AIRFLOW_BUILD_LOG_LEVEL", "INFO"))
log.setLevel(log_level)


class CustomBuild(BuilderInterface[BuilderConfig, PluginManager]):
    """Custom build class for Airflow assets and git version."""

    # Note that this name of the plugin MUST be `custom` - as long as we use it from custom
    # hatch_build.py file and not from external plugin. See note in the:
    # https://hatch.pypa.io/latest/plugins/build-hook/custom/#example
    PLUGIN_NAME = "custom"

    @staticmethod
    def clean_dir(path: Path) -> None:
        log.warning("Cleaning directory: %s", path)
        shutil.rmtree(path, ignore_errors=True)

    def clean(self, directory: str, versions: Iterable[str]) -> None:
        work_dir = Path(self.root)
        log.warning("Cleaning generated files in directory: %s", work_dir)
        airflow_package_src = work_dir / "src" / "airflow"
        airflow_ui_path = airflow_package_src / "ui"
        fastapi_ui_path = airflow_package_src / "api_fastapi" / "auth" / "managers" / "simple" / "ui"
        self.clean_dir(airflow_ui_path / "dist")
        self.clean_dir(airflow_ui_path / "node_modules")
        self.clean_dir(fastapi_ui_path / "dist")
        self.clean_dir(fastapi_ui_path / "node_modules")

    def get_version_api(self) -> dict[str, Callable[..., str]]:
        """Get custom build target for standard package preparation."""
        return {"standard": self.build_standard}

    def build_standard(self, directory: str, artifacts: Any, **build_data: Any) -> str:
        self.write_git_version()
        # run this in the parent directory of the airflow-core (i.e. airflow repo root)
        work_dir = Path(self.root).parent.resolve()
        cmd = ["prek", "run", "--hook-stage", "manual", "compile-ui-assets", "--all-files"]
        log.warning("Running command: %s", " ".join(cmd))
        run(cmd, cwd=work_dir.as_posix(), check=True)
        dist_path = Path(self.root) / "src" / "airflow" / "ui" / "dist"
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
                git_path = Path(self.root).parent.resolve() / ".git"
                log.warning("Getting git version from: %s", git_path)
                # Get git version from the git of the airflow root repo
                repo = git.Repo(str(git_path))
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
        git_version_file = Path(self.root) / "src" / "airflow" / "git_version"
        self.app.display(f"Writing version {version} to {git_version_file}")
        git_version_file.write_text(version)
