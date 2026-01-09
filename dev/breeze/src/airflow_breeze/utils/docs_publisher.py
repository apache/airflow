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
import shutil

from airflow_breeze.global_constants import get_airflow_version, get_airflowctl_version, get_task_sdk_version
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.helm_chart_utils import chart_version
from airflow_breeze.utils.packages import get_provider_distributions_metadata, get_short_package_name
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.publish_docs_helpers import pretty_format_path

PROCESS_TIMEOUT = 15 * 60

GENERATED_PATH = AIRFLOW_ROOT_PATH / "generated"


class DocsPublisher:
    """Documentation builder for Airflow Docs Publishing."""

    def __init__(self, package_name: str, output: Output | None, verbose: bool):
        self.package_name = package_name
        self.output = output
        self.verbose = verbose

    @property
    def is_versioned(self):
        """Is current documentation package versioned?"""
        # Disable versioning. This documentation does not apply to any released product and we can update
        # it as needed, i.e. with each new package of providers.
        return self.package_name not in ("apache-airflow-providers", "docker-stack")

    @property
    def _build_dir(self) -> str:
        if self.is_versioned:
            version = "stable"
            return f"{GENERATED_PATH}/_build/docs/{self.package_name}/{version}"
        return f"{GENERATED_PATH}/_build/docs/{self.package_name}"

    @property
    def _current_version(self):
        if not self.is_versioned:
            msg = (
                "This documentation package is not versioned. "
                "Make sure to add version in `provider.yaml` for the package."
            )
            raise RuntimeError(msg)
        if self.package_name == "apache-airflow":
            return get_airflow_version()
        if self.package_name.startswith("apache-airflow-providers-"):
            provider = get_provider_distributions_metadata().get(get_short_package_name(self.package_name))
            return provider["versions"][0]
        if self.package_name == "task-sdk":
            return get_task_sdk_version()
        if self.package_name == "helm-chart":
            return chart_version()
        if self.package_name == "apache-airflow-ctl":
            return get_airflowctl_version()
        raise SystemExit(f"Unsupported package: {self.package_name}")

    @property
    def _publish_dir(self) -> str:
        if self.is_versioned:
            return f"docs-archive/{self.package_name}/{self._current_version}"
        return f"docs-archive/{self.package_name}"

    def publish(self, override_versioned: bool, airflow_site_dir: str):
        """Copy documentation packages files to airflow-site repository."""
        get_console(output=self.output).print(f"Publishing docs for {self.package_name}")
        output_dir = os.path.join(airflow_site_dir, self._publish_dir)
        pretty_source = pretty_format_path(self._build_dir, os.getcwd())
        pretty_target = pretty_format_path(output_dir, airflow_site_dir)
        get_console(output=self.output).print(f"Copy directory: {pretty_source} => {pretty_target}")
        if os.path.exists(output_dir):
            if self.is_versioned:
                if override_versioned:
                    get_console(output=self.output).print(f"Overriding previously existing {output_dir}! ")
                else:
                    get_console(output=self.output).print(
                        f"Skipping previously existing {output_dir}! "
                        f"Delete it manually if you want to regenerate it!"
                    )
                    get_console(output=self.output).print()
                    return 1, f"Skipping {self.package_name}: Previously existing directory"
            # If output directory exists and is not versioned, delete it
            shutil.rmtree(output_dir)
        shutil.copytree(self._build_dir, output_dir)
        if self.is_versioned:
            with open(os.path.join(output_dir, "..", "stable.txt"), "w") as stable_file:
                stable_file.write(self._current_version)
        get_console(output=self.output).print()
        return 0, f"Docs published: {self.package_name}"
