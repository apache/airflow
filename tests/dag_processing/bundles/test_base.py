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

import fcntl
import tempfile
from pathlib import Path

import pytest

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.bundles.local import LocalDagBundle

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


def test_default_dag_storage_path():
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): ""}):
        bundle = LocalDagBundle(name="test", path="/hello")
        assert bundle._dag_bundle_root_storage_path == Path(tempfile.gettempdir(), "airflow", "dag_bundles")


class BasicBundle(BaseDagBundle):
    def refresh(self):
        pass

    def get_current_version(self):
        pass

    def path(self):
        pass


def test_dag_bundle_root_storage_path():
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): None}):
        bundle = BasicBundle(name="test")
        assert bundle._dag_bundle_root_storage_path == Path(tempfile.gettempdir(), "airflow", "dag_bundles")


def test_lock_acquisition():
    """Test that the lock context manager sets _locked and locks a lock file."""
    bundle = BasicBundle(name="locktest")
    lock_dir = bundle._dag_bundle_root_storage_path / "_locks"
    lock_file = lock_dir / f"{bundle.name}.lock"

    assert not bundle._locked

    with bundle.lock():
        assert bundle._locked
        assert lock_file.exists()

        # Check lock file is now locked
        with open(lock_file, "w") as f:
            try:
                # Try to acquire an exclusive lock in non-blocking mode.
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = False
            except OSError:
                locked = True
            assert locked

    # After, _locked is False and file unlock has been called.
    assert bundle._locked is False
    with open(lock_file, "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            unlocked = True
            fcntl.flock(f, fcntl.LOCK_UN)  # Release the lock immediately.
        except OSError:
            unlocked = False
        assert unlocked


def test_lock_exception_handling():
    """Test that exceptions within the lock context manager still release the lock."""
    bundle = BasicBundle(name="locktest")
    lock_dir = bundle._dag_bundle_root_storage_path / "_locks"
    lock_file = lock_dir / f"{bundle.name}.lock"

    try:
        with bundle.lock():
            assert bundle._locked
            raise Exception("...")
    except Exception:
        pass

    # lock file should be unlocked
    assert not bundle._locked
    with open(lock_file, "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
            fcntl.flock(f, fcntl.LOCK_UN)
        except OSError:
            acquired = False
        assert acquired
