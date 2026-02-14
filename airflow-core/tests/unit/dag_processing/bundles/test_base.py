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
from unittest.mock import call, patch

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


def test_version_lock_acquisition():
    """Test that version_lock uses a version-specific lock file."""
    bundle = BasicBundle(name="locktest", version="abc123")
    lock_dir = get_bundle_storage_root_path() / "_locks"
    lock_file = lock_dir / "locktest_version_abc123.lock"

    assert not bundle._version_locked

    with bundle.version_lock():
        assert bundle._version_locked
        assert lock_file.exists()

        # Check lock file is now locked
        with open(lock_file, "w") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = False
            except OSError:
                locked = True
            assert locked

    assert bundle._version_locked is False
    with open(lock_file, "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            unlocked = True
            fcntl.flock(f, fcntl.LOCK_UN)
        except OSError:
            unlocked = False
        assert unlocked


def test_version_lock_tracking_uses_separate_lock_file():
    """Test that version_lock without a version uses a tracking-specific lock (not the bundle lock)."""
    bundle = BasicBundle(name="locktest")
    lock_dir = get_bundle_storage_root_path() / "_locks"
    tracking_lock = lock_dir / "locktest_tracking.lock"
    bundle_lock = lock_dir / "locktest.lock"

    with bundle.version_lock():
        assert tracking_lock.exists()
        # Bundle lock file should NOT be created by version_lock
        assert not bundle_lock.exists()


def test_version_lock_exception_handling():
    """Test that exceptions within version_lock still release the lock."""
    bundle = BasicBundle(name="locktest", version="abc123")
    lock_dir = get_bundle_storage_root_path() / "_locks"
    lock_file = lock_dir / "locktest_version_abc123.lock"

    try:
        with bundle.version_lock():
            assert bundle._version_locked
            raise Exception("simulated failure")
    except Exception:
        pass

    assert not bundle._version_locked
    with open(lock_file, "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
            fcntl.flock(f, fcntl.LOCK_UN)
        except OSError:
            acquired = False
        assert acquired


def test_version_lock_reentrancy():
    """Test that nested version_lock calls don't deadlock (reentrancy check)."""
    bundle = BasicBundle(name="locktest", version="abc123")

    with bundle.version_lock():
        assert bundle._version_locked
        # Nested call should pass through without deadlock
        with bundle.version_lock():
            assert bundle._version_locked
        assert bundle._version_locked

    assert not bundle._version_locked


def test_different_versions_use_different_lock_files():
    """Test that different versions of the same bundle use different lock files."""
    bundle_v1 = BasicBundle(name="mybundle", version="v1")
    lock_dir = get_bundle_storage_root_path() / "_locks"

    with bundle_v1.version_lock():
        # v1 lock should be held
        lock_v1 = lock_dir / "mybundle_version_v1.lock"
        with open(lock_v1, "w") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                v1_locked = False
            except OSError:
                v1_locked = True
            assert v1_locked

        # v2 lock should NOT be held — different versions don't block each other
        lock_v2 = lock_dir / "mybundle_version_v2.lock"
        lock_v2.parent.mkdir(parents=True, exist_ok=True)
        lock_v2.touch()
        with open(lock_v2, "w") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                v2_free = True
                fcntl.flock(f, fcntl.LOCK_UN)
            except OSError:
                v2_free = False
            assert v2_free


def test_version_lock_different_versions_parallel():
    """Test that two different versions of the same bundle can be locked simultaneously from different threads."""
    bundle_v1 = BasicBundle(name="parallel_test", version="v1")
    bundle_v2 = BasicBundle(name="parallel_test", version="v2")
    results = {"v1_locked": False, "v2_locked": False, "v1_saw_v2": False, "v2_saw_v1": False}
    barrier = threading.Barrier(2, timeout=5)

    def lock_v1():
        with bundle_v1.version_lock():
            results["v1_locked"] = True
            barrier.wait()  # Wait for both threads to hold their locks
            results["v1_saw_v2"] = results["v2_locked"]

    def lock_v2():
        with bundle_v2.version_lock():
            results["v2_locked"] = True
            barrier.wait()  # Wait for both threads to hold their locks
            results["v2_saw_v1"] = results["v1_locked"]

    t1 = threading.Thread(target=lock_v1)
    t2 = threading.Thread(target=lock_v2)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    # Both should have been locked at the same time
    assert results["v1_locked"]
    assert results["v2_locked"]
    assert results["v1_saw_v2"], "v1 should have seen v2 locked concurrently"
    assert results["v2_saw_v1"], "v2 should have seen v1 locked concurrently"


def test_version_lock_same_version_serializes():
    """Test that two threads trying to lock the same version are serialized."""
    bundle1 = BasicBundle(name="serial_test", version="same_v")
    bundle2 = BasicBundle(name="serial_test", version="same_v")
    order = []
    lock_held = threading.Event()

    def thread1():
        with bundle1.version_lock():
            order.append("t1_start")
            lock_held.set()
            time.sleep(0.3)
            order.append("t1_end")

    def thread2():
        lock_held.wait(timeout=5)
        time.sleep(0.05)  # Ensure t1 is well inside the lock
        with bundle2.version_lock():
            order.append("t2_start")
            order.append("t2_end")

    t1 = threading.Thread(target=thread1)
    t2 = threading.Thread(target=thread2)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    # t2 should start only after t1 has finished (serialized)
    assert order == ["t1_start", "t1_end", "t2_start", "t2_end"]


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

    def test_log_exc_formats_message_correctly(self):
        """Test that _log_exc correctly formats the log message with all parameters."""
        from airflow.dag_processing.bundles.base import log as bundle_log

        bundle_name = "test_bundle"
        bundle_version = "v1.0.0"
        lock = BundleVersionLock(
            bundle_name=bundle_name,
            bundle_version=bundle_version,
        )

        test_msg = "error when attempting to acquire lock"

        with patch.object(bundle_log, "exception") as mock_exception:
            lock._log_exc(test_msg)

            assert mock_exception.mock_calls == [
                call(
                    "%s name=%s version=%s lock_file=%s",
                    test_msg,
                    bundle_name,
                    bundle_version,
                    lock.lock_file_path,
                )
            ]


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
