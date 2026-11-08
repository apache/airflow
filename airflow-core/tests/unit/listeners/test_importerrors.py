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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.dag_processing.collection import _update_import_errors
from airflow.listeners import hookimpl
from airflow.models.errors import ParseImportError

from tests_common.test_utils.db import clear_db_import_errors


@pytest.fixture(autouse=True)
def clean_import_errors():
    clear_db_import_errors()
    yield
    clear_db_import_errors()


@pytest.mark.db_test
@pytest.mark.parametrize("resolve_bundle_paths", [True, False], ids=["direct", "api"])
@mock.patch.object(ParseImportError, "full_file_path", autospec=True, return_value="/worker/a.py")
def test_import_error_hooks_support_legacy_and_bundle_identity(
    full_file_path, session, listener_manager, resolve_bundle_paths
):
    legacy_events = []
    source_events = []

    class LegacyListener:
        @hookimpl
        def on_new_dag_import_error(self, filename, stacktrace):
            legacy_events.append(("new", filename, stacktrace))

        @hookimpl
        def on_existing_dag_import_error(self, filename, stacktrace):
            legacy_events.append(("existing", filename, stacktrace))

    class SourceListener:
        @hookimpl
        def on_new_dag_import_error(self, bundle_name, relative_fileloc):
            source_events.append(("new", bundle_name, relative_fileloc))

        @hookimpl
        def on_existing_dag_import_error(self, bundle_name, relative_fileloc):
            source_events.append(("existing", bundle_name, relative_fileloc))

    listener_manager(LegacyListener())
    listener_manager(SourceListener())
    for message in ("broken", "still broken"):
        _update_import_errors(
            files_parsed=set(),
            import_errors={(name, "a.py"): message for name in ("first", "second")},
            session=session,
            resolve_bundle_paths=resolve_bundle_paths,
        )
        session.flush()

    filename = "/worker/a.py" if resolve_bundle_paths else "a.py"
    assert legacy_events == [
        (kind, filename, message)
        for kind, message in (("new", "broken"), ("existing", "still broken"))
        for _ in range(2)
    ]
    assert source_events == [
        (kind, bundle_name, "a.py") for kind in ("new", "existing") for bundle_name in ("first", "second")
    ]
    assert full_file_path.call_count == (4 if resolve_bundle_paths else 0)
