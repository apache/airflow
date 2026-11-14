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

Run as ``python -m tests_common.test_utils.testing_dags <providers>`` to print the resulting
``[dag_processor] dag_bundle_config_list`` value, where ``<providers>`` is ``all`` or a
comma-separated list of provider ids such as ``amazon,apache.hive``; Breeze does this for
``--load-provider-testing-dags``.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Collection
from pathlib import Path

from airflow.configuration import conf

from tests_common.test_utils.paths import AIRFLOW_PROVIDERS_ROOT_PATH

ALL_PROVIDERS = "all"

# Providers sit at most one namespace level below ``providers/`` (e.g. ``providers/apache/hive``).
_TESTING_DAGS_GLOBS = ("*/tests/testing_dags", "*/*/tests/testing_dags")


def parse_provider_ids(value: str) -> list[str] | None:
    """Turn ``all`` into ``None`` (every provider) and a comma-separated list into provider ids."""
    if value.strip() == ALL_PROVIDERS:
        return None
    return [provider_id.strip() for provider_id in value.split(",") if provider_id.strip()]


def find_testing_dag_folders(
    provider_ids: Collection[str] | None = None,
    providers_root: Path = AIRFLOW_PROVIDERS_ROOT_PATH,
) -> dict[str, Path]:
    """
    Map a bundle name to the ``tests/testing_dags`` folder of every selected provider that has one.

    ``provider_ids`` uses the dotted form (``apache.hive``); ``None`` selects every provider.
    """
    folders: dict[str, Path] = {}
    found_ids = set()
    for pattern in _TESTING_DAGS_GLOBS:
        for folder in providers_root.glob(pattern):
            provider_root = folder.parents[1]
            if not folder.is_dir() or not (provider_root / "provider.yaml").is_file():
                continue
            parts = provider_root.relative_to(providers_root).parts
            provider_id = ".".join(parts)
            if provider_ids is not None and provider_id not in provider_ids:
                continue
            found_ids.add(provider_id)
            folders[f"apache-airflow-providers-{'-'.join(parts)}-testing-dags"] = folder
    if provider_ids is not None and (missing := sorted(set(provider_ids) - found_ids)):
        raise ValueError(f"These providers have no tests/testing_dags folder: {', '.join(missing)}")
    return dict(sorted(folders.items()))


def build_dag_bundle_config_list(
    provider_ids: Collection[str] | None = None,
    providers_root: Path = AIRFLOW_PROVIDERS_ROOT_PATH,
) -> str:
    """Return the configured ``dag_bundle_config_list`` with a bundle appended per testing Dags folder."""
    bundle_configs = json.loads(conf.get("dag_processor", "dag_bundle_config_list"))
    bundle_configs.extend(
        {
            "name": name,
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"path": str(folder)},
        }
        for name, folder in find_testing_dag_folders(provider_ids, providers_root).items()
    )
    return json.dumps(bundle_configs)


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print(
            f"Usage: python -m tests_common.test_utils.testing_dags {ALL_PROVIDERS}|<providers>",
            file=sys.stderr,
        )
        return 2
    try:
        print(build_dag_bundle_config_list(parse_provider_ids(argv[0])))
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
