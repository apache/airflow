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
import logging
import tempfile
import threading
import time
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
import time_machine

from airflow._shared.timezones import timezone as tz
from airflow.dag_processing.bundles.base import (
    BaseDagBundle,
    BundleUsageTrackingManager,
    BundleVersionLock,
    get_bundle_storage_root_path,
)

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

log = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


@pytest.mark.parametrize(
    ("val", "expected"),
    [
        ("/blah", Path("/blah")),
        ("", Path(tempfile.gettempdir(), "airflow", "dag_bundles")),
    ],
)
def test_default_dag_storage_path(val, expected):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): val}):
        assert get_bundle_storage_root_path() == expected


class BasicBundle(BaseDagBundle):
    def refresh(self):
        pass

    def get_current_version(self):
        pass

    def path(self):
        pass


def test_dag_bundle_root_storage_path():
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): None}):
        assert get_bundle_storage_root_path() == Path(tempfile.gettempdir(), "airflow", "dag_bundles")


def test_lock_acquisition():
    """Test that the lock context manager sets _locked and locks a lock file."""
    bundle = BasicBundle(name="locktest")
    lock_dir = get_bundle_storage_root_path() / "_locks"
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
    lock_dir = get_bundle_storage_root_path() / "_locks"
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


class LockTestHelper:
    def __init__(self, num, **kwargs):
        super().__init__(**kwargs)
        self.num = num
        self.stop = None
        self.did_lock = None
        self.locker: BundleVersionLock

    def lock_the_file(self):
        self.locker = BundleVersionLock(
            bundle_name="abc",
            bundle_version="this",
        )
        with self.locker:
            self.did_lock = True
            idx = 0
            while not self.stop:
                idx += 1
                time.sleep(0.2)
                log.info("sleeping: idx=%s num=%s", idx, self.num)
        log.info("exit")


class TestBundleVersionLock:
    def test_that_shared_lock_doesnt_block_shared_lock(self):
        """Verify that two things can lock file at same time."""
        lth1 = LockTestHelper(1)
        t1 = threading.Thread(target=lth1.lock_the_file)
        lth2 = LockTestHelper(2)
        t2 = threading.Thread(target=lth2.lock_the_file)
        t1.start()
        time.sleep(0.1)
        assert lth1.did_lock is True
        t2.start()
        time.sleep(0.1)
        assert lth2.did_lock is True
        lth1.stop = True
        lth2.stop = True
        t1.join()
        t2.join()

    def test_that_shared_lock_blocks_ex_lock(self):
        """Test that exclusive lock is impossible when in bundle lock context."""
        lth1 = LockTestHelper(1)
        t1 = threading.Thread(target=lth1.lock_the_file)
        t1.start()
        time.sleep(0.1)
        assert lth1.did_lock is True
        with open(lth1.locker.lock_file_path, "a") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            fcntl.flock(f, fcntl.LOCK_UN)
            with pytest.raises(BlockingIOError):  # <-- this is the important part
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lth1.stop = True
        t1.join()

    def test_that_no_version_is_noop(self):
        with BundleVersionLock(
            bundle_name="Yer face",
            bundle_version=None,
        ) as b:
            log.info("this is fine")
        assert b.lock_file_path is None
        assert b.lock_file is None


class FakeBundle(BaseDagBundle):
    @property
    def path(self) -> Path:
        assert self.version
        return self.versions_dir / self.version

    def get_current_version(self) -> str | None: ...
    def refresh(self) -> None: ...


class TestBundleUsageTrackingManager:
    @pytest.mark.parametrize(
        ("threshold_hours", "min_versions", "when_hours", "expected_remaining"),
        [
            (3, 0, 3, 5),
            (3, 0, 6, 2),
            (10, 0, 3, 5),
            (10, 0, 6, 5),
            (0, 0, 3, 2),  # two of them are in future
            (0, 0, 6, 0),  # all of them are in past
            (0, 5, 3, 5),  # keep all no matter what
            (0, 5, 6, 5),  # keep all no matter what
            (0, 4, 3, 4),  # keep 4 no matter what
            (0, 4, 6, 4),  # keep 4 no matter what
        ],
    )
    @patch("airflow.dag_processing.bundles.base.get_bundle_tracking_dir")
    def test_that_stale_bundles_are_removed(
        self, mock_get_dir, threshold_hours, min_versions, when_hours, expected_remaining
    ):
        age_threshold = threshold_hours * 60 * 60
        with (
            conf_vars(
                {
                    ("dag_processor", "stale_bundle_cleanup_age_threshold"): str(age_threshold),
                    ("dag_processor", "stale_bundle_cleanup_min_versions"): str(min_versions),
                }
            ),
            tempfile.TemporaryDirectory() as td,
        ):
            bundle_tracking_dir = Path(td)
            mock_get_dir.return_value = bundle_tracking_dir
            h0 = tz.datetime(2025, 1, 1, 0)
            bundle_name = "abc"
            for num in range(5):
                with time_machine.travel(h0 + timedelta(hours=num), tick=False):
                    version = f"hour-{num}"
                    b = FakeBundle(version=version, name=bundle_name)
                    b.path.mkdir(exist_ok=True, parents=True)
                    with BundleVersionLock(
                        bundle_name=bundle_name,
                        bundle_version=version,
                    ):
                        print(version)
            lock_files = list(bundle_tracking_dir.iterdir())
            assert len(lock_files) == 5
            bundle_folders = list(b.versions_dir.iterdir())
            assert len(bundle_folders) == 5
            num += 1
            with time_machine.travel(h0 + timedelta(hours=when_hours), tick=False):
                BundleUsageTrackingManager()._remove_stale_bundle_versions_for_bundle(bundle_name=bundle_name)
                lock_files = list(bundle_tracking_dir.iterdir())
                assert len(lock_files) == expected_remaining
                bundle_folders = list(b.versions_dir.iterdir())
                assert len(bundle_folders) == expected_remaining
