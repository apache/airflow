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
Uninstall the providers the OpenLineage Dags do not use, before an Airflow service of the e2e stack starts.

Every task process, Dag file parse and CLI call discovers all installed providers and imports their plugins and
asset factories (google alone takes ~0.5s of CPU). With the providers of the images (~100 in the PROD image built
from sources), that is several times a task's own work on a CI runner. ``openlineage.yml`` runs this script in
every Airflow service, so assets are normalized the same way where Dags are parsed and where tasks run.

It runs in the containers without Airflow imported, and exits non-zero (failing the stack start) when a kept
provider is not installed or when providers outside ``KEPT_PROVIDERS`` are still installed afterwards. The harness
checks the providers the API server reports against ``KEPT_PROVIDERS`` too.
"""

from __future__ import annotations

import importlib.metadata
import shutil
import subprocess
import sys

PROVIDER_PREFIX = "apache-airflow-providers-"
# amazon and postgres normalize the s3:// and postgres:// asset URIs of the Dags; celery and fab run the deployment
KEPT_PROVIDERS = frozenset(
    f"{PROVIDER_PREFIX}{name}"
    for name in (
        "amazon",
        "celery",
        "common-compat",
        "common-io",
        "common-sql",
        "fab",
        "openlineage",
        "postgres",
        "standard",
    )
)


def get_installed_providers() -> set[str]:
    names = (dist.metadata["Name"].lower().replace("_", "-") for dist in importlib.metadata.distributions())
    return {name for name in names if name.startswith(PROVIDER_PREFIX)}


def uninstall(distributions: list[str]) -> None:
    # uv takes a fraction of a second where pip takes ~20s for ~100 providers, so there is no pip fallback that
    # would slow every service start down unnoticed; every image of the e2e tests has uv
    if not (uv := shutil.which("uv")):
        raise RuntimeError("uv is not installed, so the unused providers cannot be uninstalled quickly")
    subprocess.run(
        [uv, "pip", "uninstall", "--quiet", "--python", sys.executable, *distributions], check=True
    )


def main() -> int:
    installed = get_installed_providers()
    if missing := KEPT_PROVIDERS - installed:
        print(
            f"Providers the OpenLineage e2e tests need are not installed: {sorted(missing)}", file=sys.stderr
        )
        return 1
    if unused := sorted(installed - KEPT_PROVIDERS):
        uninstall(unused)
    if left := get_installed_providers() - KEPT_PROVIDERS:
        print(
            f"Unused providers are still installed after uninstalling them: {sorted(left)}", file=sys.stderr
        )
        return 1
    print(f"Uninstalled {len(unused)} providers the OpenLineage e2e tests do not use")
    return 0


if __name__ == "__main__":
    sys.exit(main())
