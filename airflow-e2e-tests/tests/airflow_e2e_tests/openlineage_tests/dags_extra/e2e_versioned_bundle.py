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
"""A minimal *versioned* DAG bundle for the OpenLineage e2e deployment.

example_openlineage_versioned_dag asserts that OpenLineage emits a non-null ``dag_bundle_version``,
which Airflow only populates when the bundle reports ``supports_versioning = True``. The real
versioned bundle (the git provider's GitDagBundle) serves DAGs from a separate bundle storage path,
which would break the ``PYTHONPATH=/opt/airflow/dags`` that the test ``VariableTransport`` class path
relies on.

This bundle instead keeps serving DAGs straight from the local dags folder (so PYTHONPATH stays
valid) and reports a deterministic, content-derived version string — enough to exercise the
version-related OpenLineage attributes without standing up a real VCS-backed bundle. The module is
copied into the dags folder by prepare_dags.py so it is importable via PYTHONPATH.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from airflow.dag_processing.bundles.local import LocalDagBundle

if TYPE_CHECKING:
    from airflow.dag_processing.bundles.base import BundleVersion


class MockVersionedLocalDagBundle(LocalDagBundle):
    """LocalDagBundle that reports a content-derived version so ``dag_bundle_version`` is populated."""

    supports_versioning = True

    # Intentionally broadens LocalDagBundle.get_current_version (which returns None, as the base
    # bundle is non-versioned) to report a real version.
    def get_current_version(self) -> str | BundleVersion:  # type: ignore[override]
        digest = hashlib.sha1(usedforsecurity=False)
        for dag_file in sorted(self.path.rglob("*.py")):
            digest.update(dag_file.relative_to(self.path).as_posix().encode())
            digest.update(str(dag_file.stat().st_size).encode())
        version = digest.hexdigest()[:12]
        try:
            # Preferred on newer cores; bare str is deprecated for versioned bundles.
            from airflow.dag_processing.bundles.base import BundleVersion

            return BundleVersion(version=version)
        except ImportError:
            # Older Airflow cores (compat runs) predate BundleVersion — a bare string still works.
            return version
