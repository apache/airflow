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
Check that there are no imports of ORM classes in any of the alembic migration scripts.
This is to prevent the addition of migration code directly referencing any ORM definition,
which could potentially break downgrades. For more details, refer to the relevant discussion
thread at this link: https://github.com/apache/airflow/issues/59871
"""

from __future__ import annotations

import importlib
import inspect
import os
from pathlib import Path
from pprint import pformat
from typing import Final

import pytest

from airflow.models.base import Base

from tests_common.test_utils.file_loading import get_imports_from_file
from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

_MIGRATIONS_DIRPATH: Final[Path] = Path(
    os.path.join(AIRFLOW_CORE_SOURCES_PATH, "airflow/migrations/versions")
)


@pytest.mark.parametrize(
    "migration_script_path",
    [pytest.param(path, id=os.path.basename(path)) for path in list(_MIGRATIONS_DIRPATH.glob("**/*.py"))],
)
def test_migration_script_has_no_orm_references(migration_script_path: Path) -> None:
    """Ensures the given alembic migration script path does not contain any ORM imports."""
    bad_imports = []
    for import_ref in get_imports_from_file(filepath=str(migration_script_path)):
        if _is_violating_orm_import(import_ref=import_ref):
            bad_imports.append(import_ref)
    assert not bad_imports, f"{str(migration_script_path)} has bad ORM imports: {pformat(bad_imports)}"


def _is_violating_orm_import(import_ref: str) -> bool:
    """Return `True` if the imported object is an ORM class from within `airflow.models`, otherwise return `False`."""
    if not import_ref.startswith("airflow.models"):
        return False
    # import the fully qualified reference to check if the reference is a subclass of a declarative base
    mod_to_import, _, attr_name = import_ref.rpartition(".")
    referenced_module = importlib.import_module(mod_to_import)
    referenced_obj = getattr(referenced_module, attr_name)
    if inspect.isclass(referenced_obj) and referenced_obj in Base.__subclasses__():
        return True
    return False
