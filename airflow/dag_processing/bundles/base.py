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
import os
import shutil
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from fcntl import LOCK_SH, flock
from operator import attrgetter
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
from pendulum.parsing import ParserError
from sqlalchemy_utils.types.enriched_datetime.pendulum_datetime import pendulum

from airflow.configuration import conf
from airflow.dag_processing.bundles.manager import DagBundlesManager

if TYPE_CHECKING:
    from pendulum import DateTime
    from structlog import BoundLogger

_log = logging.getLogger(__name__)
log: BoundLogger = structlog.wrap_logger(_log)

STALE_BUNDLE_TRACKING_FOLDER = Path(
    tempfile.gettempdir(),
    "airflow",
    "dag_bundles",
    "_tracking",
)

STALE_BUNDLE_CHECK_INTERVAL: int = conf.getint(
    "dag_processor", "stale_bundle_cleanup_interval", fallback=10 * 60
)
"""How frequently (in seconds) a worker should check for stale bundles."""

BUNDLE_STORAGE_PATH_ROOT: Path
if configured_location := conf.get("dag_processor", "dag_bundle_storage_path", fallback=None):
    BUNDLE_STORAGE_PATH_ROOT = Path(configured_location)
else:
    BUNDLE_STORAGE_PATH_ROOT = Path(tempfile.gettempdir(), "airflow", "dag_bundles")


def get_bundle_tracking_dir(bundle_name: str) -> Path:
    return STALE_BUNDLE_TRACKING_FOLDER / bundle_name


def get_bundle_tracking_file(bundle_name: str, version: str) -> Path:
    tracking_dir = get_bundle_tracking_dir(bundle_name=bundle_name)
    return Path(tracking_dir, version)


def get_bundle_base_folder(bundle_type: str, bundle_name: str) -> Path:
    return BUNDLE_STORAGE_PATH_ROOT / bundle_type / bundle_name


def get_bundle_versions_base_folder(bundle_type: str, bundle_name: str) -> Path:
    return get_bundle_base_folder(bundle_type=bundle_type, bundle_name=bundle_name) / "versions"


def get_bundle_version_path(bundle_type: str, bundle_name: str, version: str) -> Path:
    base_folder = get_bundle_versions_base_folder(bundle_type=bundle_type, bundle_name=bundle_name)
    return base_folder / version


@dataclass(frozen=True)
class TrackedBundleVersionInfo:
    """
    Internal info class for stale bundle cleanup.

    :meta private:
    """

    lock_file_path: Path
    version: str = field(compare=False)
    dt: DateTime = field(compare=False)


class BundleUsageTrackingManager:
    """
    Utility helper for removing stale bundles.

    :meta private:
    """

    def _parse_dt(self, val) -> DateTime | None:
        try:
            return pendulum.parse(val)
        except ParserError:
            return None

    @staticmethod
    def _remove_last_n(val: list[TrackedBundleVersionInfo]) -> list[TrackedBundleVersionInfo]:
        min_versions_to_keep = conf.getint(
            section="dag_processor",
            key="stale_bundle_cleanup_min_versions",
            fallback=10,
        )
        return sorted(val, key=attrgetter("dt"), reverse=True)[min_versions_to_keep:]

    @staticmethod
    def _remove_recent(val: list[TrackedBundleVersionInfo]) -> list[TrackedBundleVersionInfo]:
        age_threshold = conf.getint(
            section="dag_processor",
            key="stale_bundle_cleanup_age_threshold",
            fallback=60 * 60 * 24,
        )
        ret = []
        now = pendulum.now(tz=pendulum.UTC)
        cutoff = now - timedelta(seconds=age_threshold)
        for item in val:
            if item.dt < cutoff:
                ret.append(item)
        return ret

    def _find_all_tracking_files(self, bundle_name) -> list[TrackedBundleVersionInfo] | None:
        tracking_dir = get_bundle_tracking_dir(bundle_name=bundle_name)
        found: list[TrackedBundleVersionInfo] = []
        if not tracking_dir.exists():
            log.debug("bundle usage tracking directory does not exist", tracking_dir=tracking_dir)
            return None
        for file in tracking_dir.iterdir():
            log.debug("found bundle tracking file", path=file)
            version = file.name
            dt_str = file.read_text()
            dt = self._parse_dt(val=dt_str)
            if not dt:
                log.error(
                    "could not parse val as datetime",
                    bundle_name=bundle_name,
                    val=dt_str,
                    version=version,
                )
                continue
            found.append(TrackedBundleVersionInfo(lock_file_path=file, version=version, dt=dt))
        return found

    @staticmethod
    def _remove_stale_bundle(bundle_type: str, bundle_name: str, info: TrackedBundleVersionInfo) -> None:
        bundle_version_path = get_bundle_version_path(
            bundle_type=bundle_type,
            bundle_name=bundle_name,
            version=info.version,
        )
        log_ = log.bind(
            bundle_name=bundle_name,
            bundle_version=info.version,
            bundle_path=bundle_version_path,
            lock_file=info.lock_file_path,
        )

        try:
            log_.info("removing stale bundle")
            with open(info.lock_file_path, "a") as f:
                flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)  # exclusive lock, do not wait
                # remove the actual bundle copy
                shutil.rmtree(bundle_version_path)
                # remove the lock file
                os.remove(info.lock_file_path)
        except BlockingIOError:
            log_.info("could not obtain lock. stale bundle will not be removed.")
            return

    def _find_candidates(self, found):
        """Remove the recently used bundles."""
        candidates = self._remove_last_n(found)
        candidates = self._remove_recent(candidates)
        if log.is_enabled_for(level=logging.DEBUG):
            self._debug_candidates(candidates, found)
        return candidates

    @staticmethod
    def _debug_candidates(candidates, found):
        recently_used = list(set(found).difference(candidates))
        log_ = log.bind(candidates=candidates, recently_used=recently_used)
        if candidates:
            log_.debug("found removal candidates")
        else:
            log_.debug("no removal candidates found")

    def _remove_stale_bundle_versions_for_bundle(self, bundle_name: str, bundle_type: str):
        log.info("checking bundle for stale versions", bundle_name=bundle_name)
        found = self._find_all_tracking_files(bundle_name=bundle_name)
        if not found:
            return
        candidates = self._find_candidates(found)
        for info in candidates:
            self._remove_stale_bundle(bundle_type=bundle_type, bundle_name=bundle_name, info=info)

    def remove_stale_bundle_versions(self):
        """
        Remove bundles that are not in use and have not been used for some time.

        We will keep last N used bundles, and bundles last used with in X time.

        This isn't really necessary on worker types that don't share storage
        with other processes.
        """
        log.info("checking for stale bundle versions locally")
        bundles = list(DagBundlesManager().get_all_dag_bundles())
        for bundle in bundles:
            if not bundle.supports_versioning:
                continue
            self._remove_stale_bundle_versions_for_bundle(
                bundle_name=bundle.name, bundle_type=bundle.bundle_type
            )


class BaseDagBundle(ABC):
    """
    Base class for DAG bundles.

    DAG bundles are used both by the DAG processor and by a worker when running a task. These usage
    patterns are different, however.

    When running a task, we know what version of the bundle we need (assuming the bundle supports versioning).
    And we likely only need to keep this specific bundle version around for as long as we have tasks running using
    that bundle version. This also means, that on a single worker, it's possible that multiple versions of the same
    bundle are used at the same time.

    In contrast, the DAG processor uses a bundle to keep the DAGs from that bundle up to date. There will not be
    multiple versions of the same bundle in use at the same time. The DAG processor will always use the latest version.

    :param name: String identifier for the DAG bundle
    :param refresh_interval: How often the bundle should be refreshed from the source in seconds
        (Optional - defaults to [dag_processor] refresh_interval)
    :param version: Version of the DAG bundle (Optional)
    """

    supports_versioning: bool = False

    bundle_type: str
    """This is used in generating some the paths used for local storage."""

    _locked: bool = False

    def __init__(
        self,
        *,
        name: str,
        refresh_interval: int = conf.getint("dag_processor", "refresh_interval"),
        version: str | None = None,
    ) -> None:
        self.name = name
        self.version = version
        self.refresh_interval = refresh_interval
        self.is_initialized: bool = False

        self.base_folder = get_bundle_base_folder(bundle_name=self.name, bundle_type=self.bundle_type)
        """Base directory for all bundle files"""

        self.versions_path = get_bundle_versions_base_folder(
            bundle_type=self.bundle_type, bundle_name=self.name
        )
        """Where bundle versions are stored."""

    def initialize(self) -> None:
        """
        Initialize the bundle.

        This method is called by the DAG processor and worker before the bundle is used,
        and allows for deferring expensive operations until that point in time. This will
        only be called when Airflow needs the bundle files on disk - some uses only need
        to call the `view_url` method, which can run without initializing the bundle.

        This method must ultimately be safe to call concurrently from different threads or processes.
        If it isn't naturally safe, you'll need to make it so with some form of locking.
        There is a `lock` context manager on this class available for this purpose.
        """
        self.is_initialized = True

    @property
    @abstractmethod
    def path(self) -> Path:
        """
        Path for this bundle.

        Airflow will use this path to find/load/execute the DAGs from the bundle.
        After `initialize` has been called, all dag files in the bundle should be accessible from this path.
        """

    @abstractmethod
    def get_current_version(self) -> str | None:
        """
        Retrieve a string that represents the version of the DAG bundle.

        Airflow can use this value to retrieve this same bundle version later.
        """

    @abstractmethod
    def refresh(self) -> None:
        """
        Retrieve the latest version of the files in the bundle.

        This method must ultimately be safe to call concurrently from different threads or processes.
        If it isn't naturally safe, you'll need to make it so with some form of locking.
        There is a `lock` context manager on this class available for this purpose.
        """

    def view_url(self, version: str | None = None) -> str | None:
        """
        URL to view the bundle on an external website. This is shown to users in the Airflow UI, allowing them to navigate to this url for more details about that version of the bundle.

        This needs to function without `initialize` being called.

        :param version: Version to view
        :return: URL to view the bundle
        """

    @contextmanager
    def lock(self):
        """
        Ensure only a single bundle can enter this context at a time, by taking an exclusive lock on a lockfile.

        This is useful when a bundle needs to perform operations that are not safe to run concurrently.
        """
        if self._locked:
            yield
            return

        lock_dir_path = BUNDLE_STORAGE_PATH_ROOT / "_locks"
        lock_dir_path.mkdir(parents=True, exist_ok=True)
        lock_file_path = lock_dir_path / f"{self.name}.lock"
        with open(lock_file_path, "w") as lock_file:
            # Exclusive lock - blocks until it is available
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                self._locked = True
                yield
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                self._locked = False

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"


class BundleVersionLock:
    """
    Lock version of bundle when in use to prevent deletion.

    :meta private:
    """

    def __init__(self, bundle_name, bundle_version, **kwargs):
        super().__init__(**kwargs)
        self.bundle_name = bundle_name
        self.version = bundle_version
        self.lock_file_path = get_bundle_tracking_file(
            bundle_name=self.bundle_name,
            version=self.version,
        )
        self.lock_file = None
        self._log = log.bind(
            name=self.bundle_name,
            version=self.version,
            lock_file=self.lock_file_path,
        )

    def _update_version_file(self):
        """Create a version file containing last-used timestamp."""
        self.lock_file_path.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory() as td:
            temp_file = Path(td, self.lock_file_path)
            now = pendulum.now(tz=pendulum.UTC)
            temp_file.write_text(now.isoformat())
            os.replace(temp_file, self.lock_file_path)

    def acquire(self):
        if not self.version:
            return
        if self.lock_file:
            return
        self._update_version_file()
        self.lock_file = open(self.lock_file_path)
        flock(self.lock_file, LOCK_SH)

    def release(self):
        if self.lock_file:
            self.lock_file.close()
            self.lock_file = None

    def __enter__(self):
        # wrapping in try except here is just extra cautious since this is in task execution path
        try:
            self.acquire()
        except Exception:
            self._log.exception("error when attempting to acquire lock")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # wrapping in try except here is just extra cautious since this is in task execution path
        try:
            self.release()
        except Exception:
            self._log.exception("error when attempting to release lock")
