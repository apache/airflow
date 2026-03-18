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

import builtins
import importlib
import sys
import types
from collections.abc import Callable
from typing import Any, cast

import pytest

TARGET = "airflow.providers.common.compat.sqlalchemy.orm"


@pytest.fixture(autouse=True)
def clean_target():
    """Ensure the target module is removed from sys.modules before each test."""
    sys.modules.pop(TARGET, None)
    yield
    sys.modules.pop(TARGET, None)


def reload_target() -> Any:
    """Import the compatibility shim after the monkey‑patched environment is set."""
    return importlib.import_module(TARGET)


# ----------------------------------------------------------------------
# Helper factories for the fake sqlalchemy packages
# ----------------------------------------------------------------------
def make_fake_sqlalchemy(
    *,
    has_mapped_column: bool = False,
    column_impl: Callable[..., tuple] | None = None,
) -> tuple[Any, Any]:
    """Return a tuple `(sqlalchemy_pkg, orm_pkg)` that mimics the requested feature set."""
    # Cast the ModuleType to Any so static type checkers don't complain when we
    # dynamically add attributes like `Column`, `orm` or `mapped_column`.
    sqlalchemy_pkg = cast("Any", types.ModuleType("sqlalchemy"))
    orm_pkg = cast("Any", types.ModuleType("sqlalchemy.orm"))

    # Provide Column implementation (used by the fallback)
    if column_impl is None:
        column_impl = lambda *a, **kw: ("Column_called", a, kw)

    sqlalchemy_pkg.Column = column_impl

    if has_mapped_column:
        orm_pkg.mapped_column = lambda *a, **kw: ("mapped_column_called", a, kw)

    sqlalchemy_pkg.orm = orm_pkg
    return sqlalchemy_pkg, orm_pkg


# ----------------------------------------------------------------------
# Parametrised tests
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    ("has_mapped", "expect_fallback"),
    [
        (True, False),  # real mapped_column present
        (False, True),  # fallback to Column
    ],
)
def test_mapped_column_resolution(monkeypatch, has_mapped, expect_fallback):
    sqlalchemy_pkg, orm_pkg = make_fake_sqlalchemy(has_mapped_column=has_mapped)
    monkeypatch.setitem(sys.modules, "sqlalchemy", sqlalchemy_pkg)
    monkeypatch.setitem(sys.modules, "sqlalchemy.orm", orm_pkg)

    mod = reload_target()

    # The shim must expose a callable named `mapped_column`
    assert callable(mod.mapped_column)

    # Verify that the correct implementation is used
    result = mod.mapped_column(1, a=2)

    if expect_fallback:
        assert result == ("Column_called", (1,), {"a": 2})
    else:
        assert result == ("mapped_column_called", (1,), {"a": 2})


def test_fallback_call_shapes(monkeypatch):
    """Exercise a handful of call signatures on the fallback."""
    sqlalchemy_pkg, orm_pkg = make_fake_sqlalchemy(has_mapped_column=False)
    monkeypatch.setitem(sys.modules, "sqlalchemy", sqlalchemy_pkg)
    monkeypatch.setitem(sys.modules, "sqlalchemy.orm", orm_pkg)

    mod = reload_target()

    # No‑arg call
    assert mod.mapped_column() == ("Column_called", (), {})

    # Mixed positional / keyword
    assert mod.mapped_column(1, 2, a=3, b=4) == (
        "Column_called",
        (1, 2),
        {"a": 3, "b": 4},
    )


def test_importerror_while_importing_sqlalchemy_orm(monkeypatch):
    """Simulate an ImportError raised *during* the import of sqlalchemy.orm."""
    sqlalchemy_pkg = cast("Any", types.ModuleType("sqlalchemy"))
    sqlalchemy_pkg.Column = lambda *a, **kw: ("Column_called", a, kw)

    monkeypatch.setitem(sys.modules, "sqlalchemy", sqlalchemy_pkg)

    # Force ImportError for any attempt to import sqlalchemy.orm
    real_import = __import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("sqlalchemy.orm"):
            raise ImportError("simulated failure")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    try:
        mod = reload_target()
    finally:
        # Restore the original import function - pytest's monkeypatch will also
        # do this, but we keep the explicit finally for clarity.
        monkeypatch.setattr(builtins, "__import__", real_import)

    assert callable(mod.mapped_column)
    assert mod.mapped_column("abc") == ("Column_called", ("abc",), {})
