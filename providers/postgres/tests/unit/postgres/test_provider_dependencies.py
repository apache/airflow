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

from importlib import metadata

import pytest

DISTRIBUTION = "apache-airflow-providers-postgres"


def _default_requirements() -> list[str]:
    """Requirement strings the provider installs by default (i.e. not gated behind an extra)."""
    return [req for req in (metadata.requires(DISTRIBUTION) or []) if "extra ==" not in req]


@pytest.mark.parametrize("package", ["psycopg2-binary", "asyncpg"])
def test_backcompat_driver_stays_a_default_dependency(package):
    """
    ``psycopg2-binary`` (sync) and ``asyncpg`` (async) must remain default dependencies, not
    optional extras, while the provider supports Airflow cores older than 3.4.0. Those cores
    normalize the sync metadata-DB conn to ``postgresql+psycopg2://`` and derive the async URL as
    ``postgresql+asyncpg://`` with no psycopg fallback (and Airflow 2.11 ships SQLAlchemy 1.4,
    which has no ``psycopg`` v3 dialect), so demoting either driver to an extra silently breaks
    them. Removal is gated on bumping the floor to 3.4.0; see
    https://github.com/apache/airflow/issues/68453
    """
    defaults = _default_requirements()
    assert any(req.startswith(package) for req in defaults), (
        f"{package} must stay a default dependency of {DISTRIBUTION} until the minimum supported "
        f"Airflow is >= 3.4.0. Default dependencies were: {defaults}"
    )
