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
        fab_package_src = work_dir / "src" / "airflow" / "providers" / "fab"
        fab_ui_path = fab_package_src / "www"
        self.clean_dir(fab_ui_path / ".pnpm-store")
        self.clean_dir(fab_ui_path / "static" / "dist")
        self.clean_dir(fab_ui_path / "node_modules")
        (work_dir / "www-hash.txt").unlink(missing_ok=True)

    def get_version_api(self) -> dict[str, Callable[..., str]]:
        """Get custom build target for standard package preparation."""
        return {"standard": self.build_standard}

    def build_standard(self, directory: str, artifacts: Any, **build_data: Any) -> str:
        # run this in the airflow repo root
        work_dir = Path(self.root).parents[1].resolve()
        cmd = ["prek", "run", "compile-fab-assets", "--all-files"]
        log.warning("Running command: %s", " ".join(cmd))
        run(cmd, cwd=work_dir.as_posix(), check=True)
        dist_path = Path(self.root) / "src" / "airflow" / "providers" / "fab" / "www" / "static" / "dist"
        return dist_path.resolve().as_posix()
