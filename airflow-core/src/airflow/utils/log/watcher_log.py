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
"""Asset watcher log files on the triggerer: the capped file, its uploads and dead triggerers' files."""

from __future__ import annotations

import copy
import os
import shutil
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

import attrs
import structlog
from sqlalchemy import select

from airflow.configuration import conf
from airflow.jobs.job import Job
from airflow.models.trigger import Trigger
from airflow.utils.helpers import chunks
from airflow.utils.log.trigger_log import (
    TRIGGER_ID_KEY,
    TRIGGER_LOG_FILE_NAME,
    TRIGGER_LOG_HELD_FILE_NAME,
    TRIGGER_LOG_ROOT,
    TRIGGER_LOG_ROTATED_DIR,
    TRIGGERER_JOB_ID_KEY,
    get_trigger_log_held_path,
    get_trigger_log_part_path,
    get_trigger_log_rotated_path,
    list_trigger_log_files,
    parse_trigger_log_dir_id,
)
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.jobs.triggerer_job_runner import TriggerLoggingFactory

log: FilteringBoundLogger = structlog.get_logger(logger_name=__name__)


class CappedBytesLogger(structlog.BytesLogger):
    """
    A ``BytesLogger`` that appends to ``path`` and moves the file to the rotated directory at ``max_bytes``.

    ``may_rotate`` can postpone a rotation, ``on_rotate`` is called after each one, and ``max_bytes <= 0``
    disables the cap. If something else closes or removes the file, the next line reopens it.
    """

    __slots__ = (
        "_file_mode",
        "_folder_mode",
        "_max_bytes",
        "_may_rotate",
        "_on_rotate",
        "_path",
        "_size",
        "_torn_at",
    )

    def __init__(
        self,
        path: Path,
        max_bytes: int,
        may_rotate: Callable[[], bool] | None = None,
        on_rotate: Callable[[], None] | None = None,
    ):
        self._path, self._max_bytes = path, max_bytes
        self._may_rotate, self._on_rotate = may_rotate, on_rotate
        self._folder_mode, self._file_mode = get_log_permissions()
        super().__init__(path.open("ab"))
        self._size = self._file.tell()
        # Inode and size of the file before a write that failed part way, so the partial line can be cut.
        self._torn_at: tuple[int, int] | None = None

    @property
    def rotated_path(self) -> Path:
        return self._path.parent / TRIGGER_LOG_ROTATED_DIR / self._path.name

    def ensure_open(self) -> None:
        if self._file.closed:
            self._path.parent.mkdir(mode=self._folder_mode, parents=True, exist_ok=True)
            self._path.touch(mode=self._file_mode)
            self._file = self._path.open("ab")
            self._write = self._file.write
            self._flush = self._file.flush
            if self._torn_at is not None:
                # Cut the partial line of a failed write, which went to the triggerer's output instead, unless
                # the file has been replaced since.
                ino, size = self._torn_at
                self._torn_at = None
                if os.fstat(self._file.fileno()).st_ino == ino and self._file.tell() > size:
                    self._file.truncate(size)
                    self._file.seek(0, os.SEEK_END)
            self._size = self._file.tell()

    def _unlinked(self) -> bool:
        try:
            return os.fstat(self._file.fileno()).st_nlink == 0
        except (OSError, ValueError):
            return False

    def msg(self, message: bytes) -> None:
        with self._lock:
            try:
                if not self._file.closed and self._unlinked():
                    # Lines written to a removed file are lost.
                    with suppress(OSError):
                        self._file.close()
                self.ensure_open()
                self._write(message + b"\n")
                self._flush()
                self._size += len(message) + 1
                if 0 < self._max_bytes <= self._size and (self._may_rotate is None or self._may_rotate()):
                    self._file.close()
                    self.rotated_path.parent.mkdir(mode=self._folder_mode, parents=True, exist_ok=True)
                    with suppress(FileNotFoundError):
                        os.replace(self._path, self.rotated_path)
                    if self._on_rotate is not None:
                        try:
                            self._on_rotate()
                        except Exception:
                            # The line is already written, so a failed upload must not raise into the caller.
                            log.exception(
                                "Failed to upload a rotated trigger log", path=str(self.rotated_path)
                            )
                    self.ensure_open()  # Last, so a failure here does not stop the rotated file's upload.
            except OSError:
                # Close the file so its disk space is freed if it was removed; the next line reopens it.
                with suppress(OSError, ValueError):
                    if self._torn_at is None and not self._file.closed:
                        self._torn_at = (os.fstat(self._file.fileno()).st_ino, self._size)
                with suppress(OSError):
                    self._file.close()
                raise

    def close(self) -> None:
        # Closing flushes, and lines that cannot be flushed are lost either way.
        with suppress(OSError):
            self._file.close()

    log = debug = info = warn = warning = msg
    fatal = failure = err = error = critical = exception = msg


# Marks a dead triggerer's log directory as being uploaded by a live triggerer.
_ORPHAN_UPLOAD_CLAIM = ".uploading"
_ORPHAN_UPLOAD_CLAIM_MAX_AGE = 600.0
_TRIGGER_ID_QUERY_CHUNK_SIZE = 500
# Each thread's copy of the remote log handler with ``delete_local_copy`` on, cached for its hook (see _store).
_storing_handler = threading.local()


def get_log_permissions() -> tuple[int, int]:
    """Return the permission modes ``init_log_file`` gives new log folders and files."""
    return (
        int(conf.get("logging", "file_task_handler_new_folder_permissions", fallback="0o775"), 8),
        int(conf.get("logging", "file_task_handler_new_file_permissions", fallback="0o664"), 8),
    )


def _has_lines(path: Path) -> bool:
    try:
        return path.stat().st_size > 0
    except OSError:
        return False


def handler_keeps_no_copy(handler) -> bool:
    """
    Return whether the handler stores no copy of the file, so a file it leaves unchanged is no failed upload.

    That holds for handlers that stream each line as it is written, which have processors, and for handlers
    that take only task logs.
    """
    return bool(getattr(handler, "processors", None)) or _takes_only_task_logs(handler)


def _takes_only_task_logs(handler) -> bool:
    """Return whether the handler stores logs under an id made from their task instance, which watchers lack."""
    return hasattr(handler, "log_id_template")


def _tells_stored(handler) -> bool:
    """Return whether the handler uploads files from ``base_log_folder`` and can remove them once stored."""
    return not handler_keeps_no_copy(handler) and all(
        hasattr(handler, name) for name in ("base_log_folder", "delete_local_copy")
    )


def upload_watcher_log_file(handler, relative_path: str, path: Path) -> None:
    """
    Upload one of a watcher's files from ``path``, then empty it, or remove it if ``delete_local_logs`` is set.

    The file is emptied or removed here rather than by the handler, because the handler removes the whole
    directory, which also holds the watcher's other files that may not be stored yet.
    """
    if not _tells_stored(handler):
        handler.upload(relative_path, None)
        return
    if not _store(handler, relative_path, path):
        return
    if handler.delete_local_copy:
        path.unlink(missing_ok=True)
        with suppress(OSError):
            path.parent.rmdir()  # Only succeeds once the directory is empty.
        return
    # Empty the file, or remove it if that fails, so a later upload does not store it twice.
    try:
        os.truncate(path, 0)
    except OSError:
        path.unlink(missing_ok=True)


def _store(handler, relative_path: str, path: Path) -> bool:
    """
    Upload a link to ``path`` with ``delete_local_copy`` on, and return whether the handler stored it.

    A handler removes a file it stored when told to delete local copies, and leaves one it failed to store,
    which some report only in their own log.
    """
    if not path.is_file():
        return False
    storing = getattr(_storing_handler, "copy", None)
    if storing is None or _storing_handler.handler is not handler:
        storing = copy.copy(handler)
        storing.delete_local_copy = True
        _storing_handler.handler, _storing_handler.copy = handler, storing
    # Next to the file, so it can be linked, and so it is removed with the watcher's files if this process dies.
    root = Path(tempfile.mkdtemp(prefix=".upload-", dir=path.parent))
    try:
        link = root / relative_path
        link.parent.mkdir(parents=True)
        try:
            os.link(path, link)
        except OSError:
            shutil.copyfile(path, link)
        storing.base_log_folder = root
        storing.upload(relative_path, None)
        return not link.exists()
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _append_file(lines: Path, kept: Path) -> None:
    """Append ``lines`` to ``kept``, cutting ``kept`` back if that fails part way."""
    size = kept.stat().st_size
    try:
        with lines.open("rb") as source, kept.open("ab") as target:
            shutil.copyfileobj(source, target)
    except OSError:
        with suppress(OSError):
            os.truncate(kept, size)
        raise


def _move_into_rotated_dir(held: Path, rotated: Path) -> None:
    rotated.parent.mkdir(mode=get_log_permissions()[0], parents=True, exist_ok=True)
    os.replace(held, rotated)


def _is_left_unstored(handler, path: Path, *, raised: bool) -> bool:
    """Return whether a watcher's file must be uploaded again: it has lines the upload raised for or left."""
    return _has_lines(path) and (raised or _tells_stored(handler))


def _get_existing_trigger_ids(session, trigger_ids: set[int]) -> set[int]:
    return {
        trigger_id
        for ids in chunks(sorted(trigger_ids), _TRIGGER_ID_QUERY_CHUNK_SIZE)
        for trigger_id in session.scalars(select(Trigger.id).where(Trigger.id.in_(ids)))
    }


def get_watcher_log_max_open_files() -> int:
    """
    Return ``[triggerer] watcher_log_max_open_files`` (0 is no limit), at most a quarter of ``ulimit -n``.

    The cap leaves file descriptors for task and callback trigger logs, database connections and sockets.
    """
    setting = conf.getint("triggerer", "watcher_log_max_open_files")
    try:
        import resource  # The resource module exists only on Unix.

        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (ImportError, OSError, ValueError):
        return setting
    if soft == resource.RLIM_INFINITY:
        return setting
    limit = max(1, soft // 4)
    return limit if setting <= 0 else min(setting, limit)


@attrs.define(kw_only=True, eq=False)
class _Release:
    """The upload of a released watcher's files."""

    factory: TriggerLoggingFactory
    # The same watcher's previous release, if its upload was still pending; this upload waits for it.
    previous: _Release | None
    upload: Future = attrs.field(init=False)


@attrs.define(kw_only=True)
class WatcherLogs:
    """
    Manage the asset watcher log files of one triggerer's supervisor, apart from writing to them.

    It keeps at most ``watcher_log_max_open_files`` of them open, uploads rotated files and the files of
    released watchers off the main loop, and uploads or removes the files that dead triggerers left here.
    """

    logger_cache: dict[int, TriggerLoggingFactory]
    # Called when closing a trigger's log file fails.
    on_error: Callable[[int | None, OSError], None]
    # Trigger ids of watcher files with an open descriptor, least recently written first.
    _open_watcher_logs: dict[int, None] = attrs.field(factory=dict, init=False)
    watcher_log_max_open_files: int = attrs.field(factory=get_watcher_log_max_open_files, init=False)
    # When to scan for watcher files left by dead triggerers, and the thread running the scan.
    _orphan_scans_at: list[float] | None = attrs.field(default=None, init=False)
    _orphan_scan_thread: threading.Thread | None = attrs.field(default=None, init=False)
    # Uploads of released watchers' files; rotated files have their own pool, as release uploads wait on them.
    _upload_pool: ThreadPoolExecutor | None = attrs.field(default=None, init=False)
    _releases: dict[int, _Release] = attrs.field(factory=dict, init=False)
    # Guards ``_releases``, which upload threads also change when an upload finishes.
    _releases_lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)
    # Log paths of watchers that left this triggerer, whose files are removed once their trigger is deleted.
    _left: dict[int, str] = attrs.field(factory=dict, init=False)
    _rotated_pool: ThreadPoolExecutor | None = attrs.field(default=None, init=False)
    _pending_rotated_uploads: dict[int, Future] = attrs.field(factory=dict, init=False)
    # The last part number given to each watcher's rotated files on this triggerer.
    _rotated_parts: dict[int, int] = attrs.field(factory=dict, init=False)
    # Each watcher's last rotated file whose upload was not confirmed, resent before a rotation replaces it.
    _rotated_unconfirmed: dict[int, str] = attrs.field(factory=dict, init=False)
    # Guards the held file while a released watcher's lines are added to it or it is moved for upload.
    _held_lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)

    def note_watcher_log_write(self, trigger_id: int) -> None:
        """Record a write and close the least recently written files beyond ``watcher_log_max_open_files``."""
        self._open_watcher_logs.pop(trigger_id, None)
        self._open_watcher_logs[trigger_id] = None
        while 0 < self.watcher_log_max_open_files < len(self._open_watcher_logs):
            idle = next(iter(self._open_watcher_logs))
            del self._open_watcher_logs[idle]
            if (factory := self.logger_cache.get(idle)) is not None:
                factory.close()

    def forget_open_file(self, trigger_id: int) -> None:
        """Stop counting a trigger's file as open, as it is about to be closed."""
        self._open_watcher_logs.pop(trigger_id, None)

    def release(self, trigger_id: int, factory: TriggerLoggingFactory) -> None:
        """
        Upload and close the file of a watcher no longer assigned here, off the main loop.

        The watcher may be assigned back here at once and start a new live file, so the old lines are moved
        to a held file until the watcher's next upload here sends them.
        """
        from airflow.sdk.log import load_remote_log_handler  # noqa: SDK001

        # Upload in the background, because a triggerer can release all of its watchers at once.
        if self._upload_pool is None:
            self._upload_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="trigger-log-release")
        handler = load_remote_log_handler()
        if handler is not None and not _takes_only_task_logs(handler):
            if hasattr(factory, "bound_logger"):
                factory.close()
                self._hold(factory.bound_logger._logger._path)
        with self._releases_lock:
            release = _Release(factory=factory, previous=self._releases.get(trigger_id))
            release.upload = self._upload_pool.submit(self._upload_released, trigger_id, release)
            self._releases[trigger_id] = release

        def forget(done: Future) -> None:
            with self._releases_lock:
                if self._releases.get(trigger_id) is release:
                    self._releases.pop(trigger_id, None)

        release.upload.add_done_callback(forget)

    def _hold(self, live: Path) -> None:
        """Move a released watcher's lines to the held file, so the watcher can start a new live file here."""
        held = live.with_name(TRIGGER_LOG_HELD_FILE_NAME)
        # If this fails, the lines stay in the live file and the watcher's next upload sends them.
        with self._held_lock, suppress(OSError):
            if not _has_lines(live):
                return
            if not held.exists():
                os.replace(live, held)
                return
            # Released again before the held file was uploaded, so keep a single held file.
            _append_file(live, held)
            live.unlink()

    def _take_held(self, trigger_id: int, log_path: str) -> str | None:
        """Move the held file into the rotated directory for upload, and return its new path if it exists."""
        from airflow.sdk.log import load_remote_log_handler  # noqa: SDK001

        base = Path(conf.get("logging", "base_log_folder"))
        held = base / get_trigger_log_held_path(log_path)
        if (handler := load_remote_log_handler()) is None or not held.is_file():
            return None
        if handler_keeps_no_copy(handler):
            path = get_trigger_log_rotated_path(log_path)
        else:
            # Taking the next part number here is safe, as the watcher's rotations wait for this upload.
            part = self._rotated_parts[trigger_id] = self._rotated_parts.get(trigger_id, 0) + 1
            path = get_trigger_log_part_path(log_path, part)
        with self._held_lock:
            _move_into_rotated_dir(held, base / path)
        return path

    def _upload_released(self, trigger_id: int, release: _Release) -> None:
        if release.previous is not None:
            # Wait for the previous upload, as it reads files in the same directory.
            with suppress(Exception):
                release.previous.upload.result()
            release.previous = None
        factory = release.factory
        failed = self._upload_and_close(trigger_id, factory, keep_live=True)
        if failed:
            self._left.pop(trigger_id, None)  # The orphan scan at the next start retries the upload.
        else:
            self._left[trigger_id] = factory.log_path

    @staticmethod
    def _remove_released_files(log_path: str) -> None:
        from airflow.sdk.log import load_remote_log_handler  # noqa: SDK001

        producer_dir = (Path(conf.get("logging", "base_log_folder")) / log_path).parent
        handler = load_remote_log_handler()
        if handler is not None and _tells_stored(handler):
            if any(_has_lines(path) for path in list_trigger_log_files(producer_dir)):
                return  # Lines left mean a failed upload, which the orphan scan at the next start retries.
        shutil.rmtree(producer_dir, ignore_errors=True)
        with suppress(OSError):
            producer_dir.parent.rmdir()  # Fails while another triggerer still has a directory in it.

    def remove_left(self) -> None:
        """Remove the files of watchers that left here and whose trigger no longer exists."""
        if not (left := set(self._left)):
            return
        try:
            with create_session() as session:
                gone = left - _get_existing_trigger_ids(session, left)
        except Exception:
            log.exception("Failed to look for triggers that no longer exist")
            return
        for trigger_id in gone:
            if (release := self._releases.get(trigger_id)) is not None and not release.upload.done():
                continue
            log_path = self._left.pop(trigger_id, None)
            if log_path is None or trigger_id in self.logger_cache:
                continue  # The watcher is back here and is tracked again when it leaves.
            try:
                self._remove_released_files(log_path)
            except Exception:
                log.exception("Failed to remove the log files of a trigger that no longer exists")

    def prepare_rotation(self, trigger_id: int) -> bool:
        """Return whether the watcher's file may rotate now, first resending an unconfirmed rotated file."""
        if (release := self._releases.get(trigger_id)) is not None and not release.upload.done():
            return False  # Its released files are still being uploaded from the rotated directory.
        upload = self._pending_rotated_uploads.get(trigger_id)
        if upload is not None and not upload.done():
            return False
        if path := self._rotated_unconfirmed.pop(trigger_id, None):
            # Resend it before the next rotation replaces it, whether or not this upload succeeds.
            self._pending_rotated_uploads[trigger_id] = self._get_rotated_pool().submit(
                self._upload_rotated_again, trigger_id, path
            )
            return False
        return True

    def _get_rotated_pool(self) -> ThreadPoolExecutor:
        if self._rotated_pool is None:
            self._rotated_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="trigger-log-rotated")
        return self._rotated_pool

    @staticmethod
    def _upload_rotated_again(trigger_id: int, path: str) -> None:
        from airflow.sdk.log import load_remote_log_handler  # noqa: SDK001

        try:
            if (handler := load_remote_log_handler()) is not None:
                upload_watcher_log_file(handler, path, Path(conf.get("logging", "base_log_folder")) / path)
        except Exception:
            log.exception("Failed to upload a rotated trigger log", trigger_id=trigger_id, path=path)

    def upload_rotated(self, trigger_id: int, log_path: str) -> None:
        # Called while writing the line that rotated the file, so the upload runs in the pool instead.
        pool = self._get_rotated_pool()
        rotated = get_trigger_log_rotated_path(log_path)
        # Taking the next part number here is safe, as the watcher's next rotation waits for this upload.
        part = self._rotated_parts.get(trigger_id, 0) + 1

        def upload() -> None:
            from airflow.sdk.log import load_remote_log_handler  # noqa: SDK001

            path = rotated
            base = Path(conf.get("logging", "base_log_folder"))
            try:
                if (handler := load_remote_log_handler()) is None:
                    return
                if not handler_keeps_no_copy(handler):
                    # Each rotated file is stored as its own numbered object, counted in memory because
                    # earlier parts may be gone from disk.
                    for done in (base / rotated).parent.glob("*.log"):
                        if done.name != TRIGGER_LOG_FILE_NAME:
                            with suppress(OSError):
                                done.unlink()  # The previous part was uploaded or already retried.
                    path = get_trigger_log_part_path(log_path, part)
                    # Renamed only now, so the triggerer's log server serves it as the rotated file till then.
                    os.replace(base / rotated, base / path)
                upload_watcher_log_file(handler, path, base / path)
                raised = False
            except Exception:
                log.exception("Failed to upload a rotated trigger log", trigger_id=trigger_id, path=path)
                raised = True
            if path != rotated and _is_left_unstored(handler, base / path, raised=raised):
                self._rotated_unconfirmed[trigger_id] = path

        self._pending_rotated_uploads[trigger_id] = pool.submit(upload)
        self._rotated_parts[trigger_id] = part

    def upload_and_close(self, trigger_id: int, factory: TriggerLoggingFactory) -> None:
        if (release := self._releases.get(trigger_id)) is not None:
            # Wait for the upload of the watcher's released files, which are in the same directory.
            with suppress(Exception):
                release.upload.result()
        self._upload_and_close(trigger_id, factory)

    def _upload_and_close(self, trigger_id: int, factory: TriggerLoggingFactory, **upload_kwargs) -> bool:
        """Upload and close a trigger's file; return whether the upload raised."""
        # Imported here: the triggerer imports this module.
        from airflow.jobs.triggerer_job_runner import WatcherLoggingFactory
        from airflow.sdk.log import load_remote_log_handler  # noqa: SDK001

        if (rotated_upload := self._pending_rotated_uploads.pop(trigger_id, None)) is not None:
            rotated_upload.result()  # Else the rotated file could be uploaded again before it is emptied.
        self._rotated_unconfirmed.pop(trigger_id, None)  # The upload below sends it again.
        base = Path(conf.get("logging", "base_log_folder"))
        failed, held = False, None
        try:
            # A factory that never wrote uploads nothing, so the held file is left for a later upload.
            if isinstance(factory, WatcherLoggingFactory) and hasattr(factory, "bound_logger"):
                held = self._take_held(trigger_id, factory.log_path)
            factory.upload_to_remote(**upload_kwargs)
        except Exception:
            log.exception("Failed to upload trigger logs to remote", trigger_id=trigger_id)
            failed = True
        finally:
            # Close even if the upload failed, so the file descriptor is not leaked.
            try:
                factory.close()
            except OSError as e:
                self.on_error(trigger_id, e)
        handler = load_remote_log_handler()
        if held and _is_left_unstored(handler, base / held, raised=failed):
            # It now has a rotated file's name, so the watcher's next rotation would remove it unsent.
            self._rotated_unconfirmed[trigger_id] = held
        stores_files = handler is not None and not handler_keeps_no_copy(handler)
        if stores_files and not failed and isinstance(factory, WatcherLoggingFactory):
            # Remove the stored parts, as a rotation would, so a store that keeps them does not get them again.
            rotated_dir = (base / factory.log_path).parent / TRIGGER_LOG_ROTATED_DIR
            for part in rotated_dir.glob("*.log"):
                if part.name != TRIGGER_LOG_FILE_NAME and not _is_left_unstored(handler, part, raised=False):
                    with suppress(OSError):
                        part.unlink()
        return failed

    def upload_and_close_all(self, factories: list[tuple[int, TriggerLoggingFactory]]) -> None:
        """Upload and close these watchers' files, and wait for the released ones' uploads."""
        self._open_watcher_logs.clear()
        # In parallel, because uploading thousands of files one by one can exceed a container's stop timeout.
        with ThreadPoolExecutor(max_workers=16, thread_name_prefix="trigger-log-upload") as pool:
            list(pool.map(lambda item: self.upload_and_close(*item), factories))
        if self._upload_pool is not None:
            self._upload_pool.shutdown(wait=True)

    def handle_orphaned_trigger_logs(self, own_job_id: int | None) -> None:
        """
        Upload or remove the asset watcher files left on this disk by triggerers that are no longer alive.

        A killed triggerer uploads nothing, so its files are uploaded now and their directories removed.
        When remote logging is off or keeps no copy, the files are removed instead, except the most recently
        written for each trigger that still exists. Files of live triggerers, of triggerers without a job, and
        files whose upload failed are kept.
        """
        from airflow.sdk.log import load_remote_log_handler  # noqa: SDK001

        handler = load_remote_log_handler()
        base = Path(conf.get("logging", "base_log_folder"))
        dirs_by_job: dict[int, list[Path]] = {}
        dirs_by_trigger: dict[Path, int] = {}
        for producer_dir in base.glob(f"{TRIGGER_LOG_ROOT}/{TRIGGER_ID_KEY}=*/{TRIGGERER_JOB_ID_KEY}=*"):
            # Both ids must parse, so that only directories this module created are ever removed.
            trigger_id = parse_trigger_log_dir_id(TRIGGER_ID_KEY, producer_dir.parent.name)
            job_id = parse_trigger_log_dir_id(TRIGGERER_JOB_ID_KEY, producer_dir.name)
            if (
                trigger_id is not None
                and job_id is not None
                and job_id != own_job_id
                and producer_dir.is_dir()
            ):
                dirs_by_job.setdefault(job_id, []).append(producer_dir)
                dirs_by_trigger[producer_dir.parent] = trigger_id
        if not dirs_by_job:
            return
        with create_session() as session:
            alive = session.scalars(
                select(Job.id).where(Job.id.in_(dirs_by_job), Job.is_alive_clause(["TriggererJob"]))
            )
            dead = dirs_by_job.keys() - set(alive)
            # A watcher whose kwargs changed gets a new trigger, so all files of a deleted trigger can go.
            existing = _get_existing_trigger_ids(session, set(dirs_by_trigger.values()))
        dead_dirs = {job_id: dirs_by_job[job_id] for job_id in dead}
        if handler is not None:
            kept = {
                producer_dir: self._upload_orphaned_trigger_log_dir(handler, base, producer_dir, job_id)
                for job_id, dirs in dead_dirs.items()
                for producer_dir in dirs
            }
            # Files the handler left unchanged are then removed as if remote logging were off.
            dead_dirs = {job_id: [d for d in dirs if kept[d]] for job_id, dirs in dead_dirs.items()}
            if None in kept.values() and self._orphan_scans_at is not None:
                # The claiming triggerer may have stopped mid-scan, so scan again once its claim is stale.
                self._orphan_scans_at.append(time.monotonic() + _ORPHAN_UPLOAD_CLAIM_MAX_AGE)
        gone = {
            trigger_dir for trigger_dir, trigger_id in dirs_by_trigger.items() if trigger_id not in existing
        }
        self._remove_old_trigger_log_dirs(dead_dirs, gone=gone)

    @staticmethod
    def _upload_orphaned_trigger_log_dir(handler, base: Path, producer_dir: Path, job_id: int) -> bool | None:
        """Upload a dead triggerer's files; return True if the handler left any, None if they are claimed."""
        # Claim first, as triggerers sharing a log folder see the same files and would upload them twice.
        claim = producer_dir / _ORPHAN_UPLOAD_CLAIM
        with suppress(OSError):
            if time.time() - claim.stat().st_mtime >= _ORPHAN_UPLOAD_CLAIM_MAX_AGE:
                # Left by a triggerer that died while uploading. Moved aside, not removed: of scans taking it
                # over at once only one can move it, and one that moved the winner's new claim puts it back.
                aside = claim.with_name(f"{claim.name}.{uuid.uuid4().hex}")
                claim.rename(aside)
                try:
                    if time.time() - aside.stat().st_mtime < _ORPHAN_UPLOAD_CLAIM_MAX_AGE:
                        aside.rename(claim)
                finally:
                    aside.unlink(missing_ok=True)
        try:
            os.close(os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
        except OSError:
            return None
        kept = left = False
        try:
            # Rotated files come first, because remote handlers append in upload order.
            for local in list_trigger_log_files(producer_dir):
                # Skip empty files, which remote handlers leave after uploading them.
                if not local.is_file() or not local.stat().st_size:
                    continue
                relative_path = local.relative_to(base).as_posix()
                try:
                    path = local
                    if local.name == TRIGGER_LOG_HELD_FILE_NAME:
                        path = producer_dir / TRIGGER_LOG_ROTATED_DIR / local.name
                        _move_into_rotated_dir(local, path)
                        relative_path = path.relative_to(base).as_posix()
                    upload_watcher_log_file(handler, relative_path, path)
                except Exception:
                    log.exception("Failed to upload an orphaned trigger log", path=relative_path)
                    left = True
                    continue
                if _has_lines(path) and handler_keeps_no_copy(handler):
                    kept = left = True
                elif _has_lines(path) and _tells_stored(handler):
                    # Lines left after the upload mean it failed, so the file is kept for a later start.
                    left = True
                else:
                    log.info(
                        "Uploaded a trigger log left behind by a triggerer that is not alive",
                        path=relative_path,
                        triggerer_job_id=job_id,
                    )
            if not left:
                # Remove the uploaded files here, as the handler keeps them without ``delete_local_logs``.
                shutil.rmtree(producer_dir, ignore_errors=True)
                with suppress(OSError):
                    producer_dir.parent.rmdir()  # Fails while another triggerer still has a directory in it.
        finally:
            claim.unlink(missing_ok=True)
        return kept

    @staticmethod
    def _remove_old_trigger_log_dirs(dead_dirs: dict[int, list[Path]], *, gone: set[Path]) -> None:
        """Remove dead triggerers' files, keeping the most recently written of each trigger not ``gone``."""

        def last_written(producer_dir: Path) -> float:
            # Not the highest job id: on a shared log folder an older triggerer can hold the newest lines.
            mtimes = [0.0]
            # Nothing is claimed without remote logging, so another triggerer may remove these meanwhile.
            with suppress(OSError):
                for f in producer_dir.rglob("*"):
                    with suppress(OSError):
                        if f.is_file():
                            mtimes.append(f.stat().st_mtime)
            return max(mtimes)

        dead_dirs_by_trigger: dict[Path, list[tuple[float, int, Path]]] = {}
        for job_id, dirs in dead_dirs.items():
            for producer_dir in dirs:
                dead_dirs_by_trigger.setdefault(producer_dir.parent, []).append(
                    (last_written(producer_dir), job_id, producer_dir)
                )
        for trigger_dir, dirs_of_trigger in dead_dirs_by_trigger.items():
            is_gone = trigger_dir in gone
            for _, job_id, producer_dir in sorted(dirs_of_trigger)[: None if is_gone else -1]:
                shutil.rmtree(producer_dir, ignore_errors=True)
                log.info(
                    "Removed a trigger log left behind by a triggerer that is not alive",
                    path=producer_dir.as_posix(),
                    triggerer_job_id=job_id,
                )
            if is_gone:
                with suppress(OSError):
                    trigger_dir.rmdir()  # Fails while a live triggerer still has a directory in it.

    def maybe_handle_orphaned_trigger_logs(self, own_job_id: int | None) -> None:
        # Scan at start, and again once a triggerer that died just before this start stops counting as alive.
        # It runs in a thread, so slow uploads do not stop this triggerer heartbeating and handling events.
        now = time.monotonic()
        if self._orphan_scans_at is None:
            self._orphan_scans_at = [
                now,
                now + conf.getfloat("triggerer", "triggerer_health_check_threshold"),
            ]
        if not self._orphan_scans_at or now < self._orphan_scans_at[0]:
            return
        if self._orphan_scan_thread is not None and self._orphan_scan_thread.is_alive():
            return
        self._orphan_scans_at.pop(0)
        self._orphan_scan_thread = threading.Thread(
            target=self._handle_orphaned_trigger_logs_logged,
            args=(own_job_id,),
            name="orphaned-trigger-logs",
            daemon=True,
        )
        self._orphan_scan_thread.start()

    def _handle_orphaned_trigger_logs_logged(self, own_job_id: int | None) -> None:
        try:
            self.handle_orphaned_trigger_logs(own_job_id)
        except Exception:
            log.exception("Failed to look for orphaned trigger logs")
