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
"""
Expose the ``tests/testing_dags`` folders of providers as Dag bundles.

Run as ``python -m tests_common.test_utils.testing_dags`` to print the resulting
``[dag_processor] dag_bundle_config_list`` value; Breeze does this for ``--load-testing-dags``.
"""

from __future__ import annotations

import json
from pathlib import Path

from airflow.configuration import conf

from tests_common.test_utils.paths import AIRFLOW_PROVIDERS_ROOT_PATH

# Providers sit at most one namespace level below ``providers/`` (e.g. ``providers/apache/hive``).
_TESTING_DAGS_GLOBS = ("*/tests/testing_dags", "*/*/tests/testing_dags")


def find_testing_dag_folders(providers_root: Path = AIRFLOW_PROVIDERS_ROOT_PATH) -> dict[str, Path]:
    """Map a bundle name to the ``tests/testing_dags`` folder of every provider that has one."""
    folders = {}
    for pattern in _TESTING_DAGS_GLOBS:
        for folder in providers_root.glob(pattern):
            provider_root = folder.parents[1]
            if not folder.is_dir() or not (provider_root / "provider.yaml").is_file():
                continue
            provider_id = "-".join(provider_root.relative_to(providers_root).parts)
            folders[f"apache-airflow-providers-{provider_id}-testing-dags"] = folder
    return dict(sorted(folders.items()))


def build_dag_bundle_config_list(providers_root: Path = AIRFLOW_PROVIDERS_ROOT_PATH) -> str:
    """Return the configured ``dag_bundle_config_list`` with a bundle appended per testing Dags folder."""
    bundle_configs = json.loads(conf.get("dag_processor", "dag_bundle_config_list"))
    bundle_configs.extend(
        {
            "name": name,
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"path": str(folder)},
        }
        for name, folder in find_testing_dag_folders(providers_root).items()
    )
    return json.dumps(bundle_configs)


if __name__ == "__main__":
    print(build_dag_bundle_config_list())
