#
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
"""Pytest harness for migration anti-pattern checks (MIG001/MIG002/MIG003)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_PREK_SCRIPT = Path(__file__).resolve().parents[4] / "scripts" / "ci" / "prek" / "check_migration_patterns.py"
_spec = importlib.util.spec_from_file_location("check_migration_patterns", _PREK_SCRIPT)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Could not load prek script: {_PREK_SCRIPT}")
_mod = importlib.util.module_from_spec(_spec)
# Register in sys.modules before exec so dataclasses can resolve the module via cls.__module__
sys.modules["check_migration_patterns"] = _mod
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]  # typeshed: Loader lacks exec_module
MigrationFile = _mod.MigrationFile
check_mig001 = _mod.check_mig001
check_mig002 = _mod.check_mig002
check_mig003 = _mod.check_mig003

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "src" / "airflow" / "migrations" / "versions"


@pytest.fixture(
    scope="module",
    params=sorted(MIGRATIONS_DIR.glob("*.py")),
    ids=lambda p: p.name,
)
def parsed_migration(request):
    return MigrationFile.from_path(request.param)


def test_mig001_no_dml_before_sqlite_fkeys_guard(parsed_migration):
    """MIG001: No DML via op.execute() should appear before disable_sqlite_fkeys."""
    errors = check_mig001(parsed_migration)
    assert not errors, "\n".join(errors)


def test_mig002_no_ddl_before_sqlite_fkeys_guard(parsed_migration):
    """MIG002: No DDL via op.*() should appear before disable_sqlite_fkeys."""
    errors = check_mig002(parsed_migration)
    assert not errors, "\n".join(errors)


def test_mig003_dml_requires_offline_mode_guard(parsed_migration):
    """MIG003: DML via op.execute() requires context.is_offline_mode() guard."""
    errors = check_mig003(parsed_migration)
    assert not errors, "\n".join(errors)
