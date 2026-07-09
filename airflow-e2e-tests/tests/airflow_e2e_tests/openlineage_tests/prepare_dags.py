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
Generate the dags folder for the OpenLineage e2e deployment from the provider system tests.

The OpenLineage system-test DAGs in ``providers/openlineage/tests/system/openlineage`` are the
single source of truth. They are *designed* to run in-process via pytest, so each one ends with a
``get_test_run`` block that imports ``tests_common`` — which is not present in the PROD image. This
copies the whole ``system`` package into the e2e dags folder and strips that pytest-only footer so
the DAGs parse and run in a real deployment.

The package layout is preserved as ``<dags>/system/openlineage/...`` and the dags folder is added to
``PYTHONPATH`` in docker-compose, so the DAGs' ``from system.openlineage... import ...`` and the
dotted-path ``VariableTransport`` both resolve unchanged — no import rewriting needed.
"""

from __future__ import annotations

import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
AIRFLOW_ROOT_PATH = HERE.parents[4]

SYSTEM_TESTS_SOURCE = AIRFLOW_ROOT_PATH / "providers" / "openlineage" / "tests" / "system"
# Harness-only modules (warmup DAG, versioned bundle) that are not part of the provider system tests.
DAGS_EXTRA_SOURCE = HERE / "dags_extra"

# The marker after which everything is Airflow's in-process test harness (pytest-only).
PYTEST_FOOTER_MARKER = "from tests_common.test_utils.system_tests import get_test_run"


def _strip_pytest_footer(dag_file: Path) -> None:
    lines = dag_file.read_text().splitlines(keepends=True)
    kept: list[str] = []
    for line in lines:
        if PYTEST_FOOTER_MARKER in line:
            break
        kept.append(line)
    dag_file.write_text("".join(kept))


def prepare_dags(dest: Path) -> Path:
    """Populate ``dest`` with the OpenLineage e2e dags from the provider system tests."""
    if not SYSTEM_TESTS_SOURCE.is_dir():
        raise FileNotFoundError(f"OpenLineage system tests not found at {SYSTEM_TESTS_SOURCE}")

    dest.mkdir(parents=True, exist_ok=True)

    # Copy the whole `system` package so `system.openlineage.{operator,transport,expected_events}`
    # imports resolve from the dags folder.
    shutil.copytree(SYSTEM_TESTS_SOURCE, dest / "system", dirs_exist_ok=True)

    openlineage_dir = dest / "system" / "openlineage"

    # The pytest conftest imports `pytest`, which is absent from the PROD image; drop it so the
    # dag-processor does not choke on it.
    (openlineage_dir / "conftest.py").unlink(missing_ok=True)

    for dag_file in openlineage_dir.glob("example_openlineage_*.py"):
        _strip_pytest_footer(dag_file)

    for extra_file in DAGS_EXTRA_SOURCE.glob("*.py"):
        shutil.copy2(extra_file, dest / extra_file.name)

    # example_openlineage_docs_file_dag uses doc_md="dag_doc.md" and the expected event's
    # documentation.description is exactly "# MD doc file". The provider's own dag_doc.md carries an
    # Apache license header (required for repo files), so OpenLineage would emit the header too.
    # Generate a clean, license-free copy here (it lives only in the gitignored dags folder, never in
    # the repo); docker-compose mounts it into the parsing CWD so doc_md resolves to this content.
    (dest / "dag_doc.md").write_text("# MD doc file")

    return dest
