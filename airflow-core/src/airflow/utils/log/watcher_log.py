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
"""An asset watcher's own log file on the triggerer: the capped file, its uploads and the orphan scan."""

from __future__ import annotations

import os
import shutil
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

import attrs
import structlog
from sqlalchemy import select

from airflow.configuration import conf
from airflow.models.trigger import Trigger
from airflow.utils.helpers import chunks
from airflow.utils.log.trigger_log import (
    TRIGGER_ID_KEY,
    TRIGGER_LOG_FILE_NAME,
    TRIGGER_LOG_ROOT,
    TRIGGER_LOG_ROTATED_DIR,
    TRIGGERER_JOB_ID_KEY,
    get_trigger_log_part_path,
    get_trigger_log_rotated_path,
    list_trigger_log_files,
    parse_trigger_log_dir_id,
)
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.jobs.triggerer_job_runner import TriggerLoggingFactory, WatcherLoggingFactory

log: FilteringBoundLogger = structlog.get_logger(logger_name=__name__)


class _CappedBytesLogger(structlog.BytesLogger):
    """
    ``BytesLogger`` appending to ``path``, moved to the rotated directory once it reaches ``max_bytes``.

    ``may_rotate`` can hold a rotation off, ``on_rotate`` is called after one; ``max_bytes <= 0`` is no
    cap. A closed or removed file (a log cleaner, ``delete_local_logs``, or on a shared log folder another
    triggerer's orphan scan) is reopened by the next line.
    """

    __slots__ = ("_max_bytes", "_may_rotate", "_on_rotate", "_path", "_size")

    def __init__(
        self,
        path: Path,
        max_bytes: int,
        may_rotate: Callable[[], bool] | None = None,
        on_rotate: Callable[[], None] | None = None,
    ):
        self._path, self._max_bytes = path, max_bytes
        self._may_rotate, self._on_rotate = may_rotate, on_rotate
        super().__init__(path.open("ab"))
        self._size = self._file.tell()

    @property
    def rotated_path(self) -> Path:
        return self._path.parent / TRIGGER_LOG_ROTATED_DIR / self._path.name

    def ensure_open(self) -> None:
        if not self._file.closed and self._unlinked():
            # Lines written to a removed file are lost.
            with suppress(OSError):
                self._file.close()
        if self._file.closed:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._file = self._path.open("ab")
            self._write = self._file.write
            self._flush = self._file.flush
            self._size = self._file.tell()

    def _unlinked(self) -> bool:
        try:
            return os.fstat(self._file.fileno()).st_nlink == 0
        except (OSError, ValueError):
            return False

    def msg(self, message: bytes) -> None:
        with self._lock:
            try:
                self.ensure_open()
                self._write(message + b"\n")
                self._flush()
                self._size += len(message) + 1
                if 0 < self._max_bytes <= self._size and (self._may_rotate is None or self._may_rotate()):
                    self._file.close()
                    self.rotated_path.parent.mkdir(parents=True, exist_ok=True)
                    with suppress(FileNotFoundError):
                        os.replace(self._path, self.rotated_path)
                    self.ensure_open()
                    if self._on_rotate is not None:
                        try:
                            self._on_rotate()
                        except Exception:
                            # The line is written: the caller, reading the runner's logs, must not see this.
                            log.exception(
                                "Failed to upload a rotated trigger log", path=str(self.rotated_path)
                            )
            except OSError:
                # Reopened by the next line: a file removed meanwhile (to free a full disk) frees its space.
                with suppress(OSError):
                    self._file.close()
                raise

    def close(self) -> None:
        # Closing flushes; on a full disk those lines are lost either way.
        with suppress(OSError):
            self._file.close()

    log = debug = info = warn = warning = msg
    fatal = failure = err = error = critical = exception = msg


# Marks a dead triggerer's log directory that one triggerer is uploading (see handle_orphaned_trigger_logs).
_ORPHAN_UPLOAD_CLAIM = ".uploading"
_ORPHAN_UPLOAD_CLAIM_MAX_AGE = 600.0
_TRIGGER_ID_QUERY_CHUNK_SIZE = 500
# Remote log handlers that take only task logs: a watcher's file is left as it was however often it is sent.
_TASK_LOGS_ONLY_HANDLERS = frozenset({"ElasticsearchRemoteLogIO", "OpensearchRemoteLogIO"})
# Remote log handlers that empty a file they stored and only log a failed write: lines left mean it failed.
_EMPTIES_STORED_FILE_HANDLERS = frozenset({"S3RemoteLogIO", "GCSRemoteLogIO", "WasbRemoteLogIO"})
# Remote log handlers that append to the stored object: a file sent again would be stored twice.
_APPENDING_HANDLERS = frozenset({"OSSRemoteLogIO"})
# Remote log handlers that need none of the above. Any handler not named in these lists is logged once (see
# _handler_is): a renamed one would otherwise silently lose what its name was listed for.
# CloudWatch and Cloud Logging stream each line as it is written (Stackdriver is a Cloud Logging subclass).
_PLAIN_HANDLERS = frozenset({"HdfsRemoteLogIO", "CloudWatchRemoteLogIO", "CloudLoggingRemoteLogIO"})
_KNOWN_HANDLERS = (
    _TASK_LOGS_ONLY_HANDLERS | _EMPTIES_STORED_FILE_HANDLERS | _APPENDING_HANDLERS | _PLAIN_HANDLERS
)
_checked_handler_classes: set[type] = set()


def _has_lines(path: Path) -> bool:
    try:
        return path.stat().st_size > 0
    except OSError:
        return False


def _handler_keeps_no_copy(handler) -> bool:
    """
    Whether a file the handler leaves as it was after ``upload`` is no sign of a failed upload.

    True for a handler that streams each line as it is written (it has processors, like CloudWatch) or that takes
    only task logs (Elasticsearch, OpenSearch; by class name, as nothing else tells them apart). Any other handler
    that leaves the file as it was failed to upload it (S3, GCS and WASB report that only in their log).
    """
    if getattr(handler, "processors", None):
        return True
    return _handler_is(handler, _TASK_LOGS_ONLY_HANDLERS)


def _handler_is(handler, names: frozenset[str]) -> bool:
    """Whether the handler's class, or a base class, has one of these names (as for the above)."""
    classes = type(handler).__mro__
    if type(handler) not in _checked_handler_classes:
        _checked_handler_classes.add(type(handler))
        if not any(cls.__name__ in _KNOWN_HANDLERS for cls in classes):
            log.info(
                "Remote log handler not known by name: asset watcher log files are sent to it as to one "
                "that stores them, and sent again only if the upload raises",
                handler=f"{type(handler).__module__}.{type(handler).__qualname__}",
            )
    return any(cls.__name__ in names for cls in classes)


def get_watcher_log_max_open_files() -> int:
    """
    Return ``[triggerer] watcher_log_max_open_files`` (0: no limit), at most a quarter of ``ulimit -n``.

    The task and callback trigger logs, the database and the sockets need descriptors too.
    """
    setting = conf.getint("triggerer", "watcher_log_max_open_files")
    try:
        import resource  # Unix only

        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (ImportError, OSError, ValueError):
        return setting
    if soft == resource.RLIM_INFINITY:
        return setting
    limit = max(1, soft // 4)
    return limit if setting <= 0 else min(setting, limit)


@attrs.define(kw_only=True)
class WatcherLogs:
    """
    The asset watcher log files of one triggerer's supervisor, other than writing to them.

    Keeps at most ``watcher_log_max_open_files`` of them open, uploads their rotated files and those of
    released watchers off the main loop, and uploads or removes the files triggerers that died left here.
    """

    # The supervisor's trigger log factories, by trigger id
    logger_cache: dict[int, TriggerLoggingFactory]
    # Told when closing a trigger's log file fails
    on_error: Callable[[int | None, OSError], None]
    # Asset watcher files with an open descriptor, least recently written first
    _open_watcher_logs: dict[int, None] = attrs.field(factory=dict, init=False)
    watcher_log_max_open_files: int = attrs.field(factory=get_watcher_log_max_open_files, init=False)
    # When to look for watcher files left by triggerers that died, and the thread doing it
    _orphan_scans_at: list[float] | None = attrs.field(default=None, init=False)
    _orphan_scan_thread: threading.Thread | None = attrs.field(default=None, init=False)
    # Uploads of released watchers' files, and of rotated files in their own pool: release uploads wait
    # for them
    _upload_pool: ThreadPoolExecutor | None = attrs.field(default=None, init=False)
    _pending_uploads: dict[int, Future] = attrs.field(factory=dict, init=False)
    # The released watchers' factories while their upload is pending: one back before it starts keeps it
    _released_factories: dict[int, WatcherLoggingFactory] = attrs.field(factory=dict, init=False)
    _rotated_pool: ThreadPoolExecutor | None = attrs.field(default=None, init=False)
    _pending_rotated_uploads: dict[int, Future] = attrs.field(factory=dict, init=False)
    # The last number given to each watcher's rotated files here (see upload_rotated)
    _rotated_parts: dict[int, int] = attrs.field(factory=dict, init=False)
    # Each watcher's last rotated file whose upload the handler did not confirm, sent again before it is replaced
    _rotated_unconfirmed: dict[int, str] = attrs.field(factory=dict, init=False)

    def note_watcher_log_write(self, trigger_id: int) -> None:
        """Keep at most ``watcher_log_max_open_files`` watcher files open, closing the least recent."""
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

    def release(self, trigger_id: int, factory: WatcherLoggingFactory) -> None:
        """Upload and close the file of a watcher no longer assigned here, off the main loop."""
        # In the background: a triggerer that lost its watchers (a DB partition) releases them all at once.
        if self._upload_pool is None:
            self._upload_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="trigger-log-release")
        upload = self._upload_pool.submit(self.upload_and_close, trigger_id, factory)
        self._pending_uploads[trigger_id] = upload
        self._released_factories[trigger_id] = factory

        def forget(done: Future) -> None:
            if self._pending_uploads.get(trigger_id) is done:
                self._pending_uploads.pop(trigger_id, None)
                self._released_factories.pop(trigger_id, None)

        upload.add_done_callback(forget)

    def take_back(self, trigger_id: int) -> tuple[bool, WatcherLoggingFactory | None]:
        """Return whether a watcher assigned here again may log yet, and its released factory if it keeps it."""
        if (upload := self._pending_uploads.get(trigger_id)) is None or upload.done():
            return True, None
        released = self._released_factories.get(trigger_id)
        if released is None or not upload.cancel():
            # Back while its file uploads: the handler empties or removes it once uploaded, which would lose
            # the new lines.
            return False, None
        # Its upload had not started (queued behind others, minutes each with an unreachable store): keep the
        # same file, uploaded when the watcher leaves again or the triggerer stops.
        return True, released

    def rotated_upload_done(self, trigger_id: int) -> bool:
        upload = self._pending_rotated_uploads.get(trigger_id)
        if upload is not None and not upload.done():
            return False
        if self._rotated_pool is not None and (path := self._rotated_unconfirmed.pop(trigger_id, None)):
            # Now, while it is the only file in its directory: with delete_local_logs, a handler deletes the
            # directory of the file it uploaded. Replaced by the next rotation whether it goes through or not.
            self._pending_rotated_uploads[trigger_id] = self._rotated_pool.submit(
                self._upload_rotated_again, trigger_id, path
            )
            return False
        return True

    @staticmethod
    def _upload_rotated_again(trigger_id: int, path: str) -> None:
        from airflow.sdk.log import load_remote_log_handler  # noqa: SDK001

        try:
            if (handler := load_remote_log_handler()) is not None:
                handler.upload(path, None)
        except Exception:
            log.exception("Failed to upload a rotated trigger log", trigger_id=trigger_id, path=path)

    def upload_rotated(self, trigger_id: int, log_path: str) -> None:
        # Called by the line that rotated the file: the rest runs in the pool, not to hold up the runner's logs.
        if self._rotated_pool is None:
            self._rotated_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="trigger-log-rotated")
        rotated = get_trigger_log_rotated_path(log_path)
        # Numbered here: a watcher's next rotation waits for this upload (see rotated_upload_done).
        part = self._rotated_parts.get(trigger_id, 0) + 1

        def upload() -> None:
            from airflow.sdk.log import load_remote_log_handler  # noqa: SDK001

            path = rotated
            base = Path(conf.get("logging", "base_log_folder"))
            try:
                if (handler := load_remote_log_handler()) is None:
                    return
                if not _handler_keeps_no_copy(handler):
                    # A handler that stores files gets each rotated file as its own object (see
                    # get_trigger_log_part_path), numbered by this triggerer, the only one writing there.
                    # Counted, not read from the disk: a watcher that comes back gets a new file, and the parts
                    # it uploaded before are gone locally.
                    for done in (base / rotated).parent.glob("*.log"):
                        if done.name != TRIGGER_LOG_FILE_NAME:
                            with suppress(OSError):
                                done.unlink()  # the previous one, uploaded or tried twice: one at a time
                    path = get_trigger_log_part_path(log_path, part)
                    # Named only now: until then the triggerer's log server serves it as the rotated file.
                    os.replace(base / rotated, base / path)
                handler.upload(path, None)
                raised = False
            except Exception:
                log.exception("Failed to upload a rotated trigger log", trigger_id=trigger_id, path=path)
                raised = True
            if path != rotated and _has_lines(base / path):
                # Sent again if the upload raised, or if S3, GCS or WASB left lines (others may keep them).
                if (raised and not _handler_is(handler, _APPENDING_HANDLERS)) or (
                    not raised and _handler_is(handler, _EMPTIES_STORED_FILE_HANDLERS)
                ):
                    self._rotated_unconfirmed[trigger_id] = path

        self._pending_rotated_uploads[trigger_id] = self._rotated_pool.submit(upload)
        self._rotated_parts[trigger_id] = part

    def upload_and_close(self, trigger_id: int, factory: TriggerLoggingFactory) -> None:
        if (rotated_upload := self._pending_rotated_uploads.pop(trigger_id, None)) is not None:
            rotated_upload.result()  # else the rotated file could be read again before it is emptied
        self._rotated_unconfirmed.pop(trigger_id, None)  # sent again below, with the live file
        try:
            factory.upload_to_remote()
        except Exception:
            log.exception("Failed to upload trigger logs to remote", trigger_id=trigger_id)
        finally:
            # Close even if the upload failed, not to leak the descriptor. Closing flushes, which can fail.
            try:
                factory.close()
            except OSError as e:
                self.on_error(trigger_id, e)

    def upload_and_close_all(self, factories: list[tuple[int, TriggerLoggingFactory]]) -> None:
        """Upload and close these watchers' files, and wait for the released ones' uploads."""
        self._open_watcher_logs.clear()
        # Side by side: one after the other, thousands of files take longer than a container's stop timeout.
        with ThreadPoolExecutor(max_workers=16, thread_name_prefix="trigger-log-upload") as pool:
            list(pool.map(lambda item: self.upload_and_close(*item), factories))
        if self._upload_pool is not None:
            self._upload_pool.shutdown(wait=True)

    def handle_orphaned_trigger_logs(self, own_job_id: int | None) -> None:
        """
        Upload, or remove, the asset watcher files left on this disk by triggerers that are no longer alive.

        A killed triggerer uploads nothing, so its files are uploaded now, then their directories removed.
        Files remote logging doesn't take (it is off, or the handler keeps no copy, like Elasticsearch) are
        removed instead: all but the most recently written for each watcher, and all for triggers that no
        longer exist. Files of live triggerers, this one included, of triggerers without a job (no job id
        directory), and those a failed upload left stay.
        """
        from airflow.jobs.job import Job
        from airflow.sdk.log import load_remote_log_handler  # noqa: SDK001

        handler = load_remote_log_handler()
        base = Path(conf.get("logging", "base_log_folder"))
        dirs_by_job: dict[int, list[Path]] = {}
        dirs_by_trigger: dict[Path, int] = {}
        for producer_dir in base.glob(f"{TRIGGER_LOG_ROOT}/{TRIGGER_ID_KEY}=*/{TRIGGERER_JOB_ID_KEY}=*"):
            # Only directories this module names are ever removed: both ids must parse.
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
        trigger_ids = sorted(set(dirs_by_trigger.values()))
        with create_session() as session:
            jobs = session.scalars(select(Job).where(Job.id.in_(dirs_by_job)))
            dead = dirs_by_job.keys() - {job.id for job in jobs if job.is_alive()}
            # A watcher whose kwargs changed got a new trigger; the old id's files have nothing left to keep.
            existing = {
                trigger_id
                for ids in chunks(trigger_ids, _TRIGGER_ID_QUERY_CHUNK_SIZE)
                for trigger_id in session.scalars(select(Trigger.id).where(Trigger.id.in_(ids)))
            }
        dead_dirs = {job_id: dirs_by_job[job_id] for job_id in dead}
        if handler is not None:
            # What the handler left as it was is then removed as without remote logging.
            dead_dirs = {
                job_id: [d for d in dirs if self._upload_orphaned_trigger_log_dir(handler, base, d, job_id)]
                for job_id, dirs in dead_dirs.items()
            }
        gone = {
            trigger_dir for trigger_dir, trigger_id in dirs_by_trigger.items() if trigger_id not in existing
        }
        self._remove_old_trigger_log_dirs(dead_dirs, gone=gone)

    @staticmethod
    def _upload_orphaned_trigger_log_dir(handler, base: Path, producer_dir: Path, job_id: int) -> bool:
        """Upload a dead triggerer's files unless another triggerer is; True if the handler left any."""
        # Claimed first: triggerers starting together on a shared log folder see the same files, and remote
        # handlers append, so a file uploaded twice would be in the remote copy twice.
        claim = producer_dir / _ORPHAN_UPLOAD_CLAIM
        with suppress(OSError):
            if time.time() - claim.stat().st_mtime >= _ORPHAN_UPLOAD_CLAIM_MAX_AGE:
                claim.unlink()  # left by a triggerer that died while uploading
        try:
            os.close(os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY))
        except OSError:
            return False
        kept = left = False
        try:
            # The rotated files first: remote handlers append in upload order.
            for path in list_trigger_log_files(producer_dir):
                # Remote handlers empty a file they uploaded unless they delete it; skip those.
                if not path.is_file() or not path.stat().st_size:
                    continue
                relative_path = path.relative_to(base).as_posix()
                try:
                    handler.upload(relative_path, None)
                except Exception:
                    log.exception("Failed to upload an orphaned trigger log", path=relative_path)
                    left = True
                    continue
                if _has_lines(path) and _handler_keeps_no_copy(handler):
                    kept = left = True
                elif _has_lines(path) and _handler_is(handler, _EMPTIES_STORED_FILE_HANDLERS):
                    # Lines S3, GCS or WASB left are a failed upload, kept for a later start. Other stores keep
                    # the file, so as in upload_rotated only an upload that raised failed.
                    left = True
                else:
                    log.info(
                        "Uploaded a trigger log left behind by a triggerer that is not alive",
                        path=relative_path,
                        triggerer_job_id=job_id,
                    )
            if not left:
                # All uploaded. Without delete_local_logs the directories stay, and with HDFS or OSS the files.
                shutil.rmtree(producer_dir, ignore_errors=True)
                with suppress(OSError):
                    producer_dir.parent.rmdir()  # not while another triggerer still has a directory in it
        finally:
            claim.unlink(missing_ok=True)
        return kept

    @staticmethod
    def _remove_old_trigger_log_dirs(dead_dirs: dict[int, list[Path]], *, gone: set[Path]) -> None:
        """Remove dead triggerers' files, keeping the most recently written of each trigger not ``gone``."""

        def last_written(producer_dir: Path) -> float:
            # Not the highest job id: on a shared log folder, a watcher taken over by an older triggerer has
            # its newest lines there.
            return max((f.stat().st_mtime for f in producer_dir.rglob("*") if f.is_file()), default=0.0)

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
                    trigger_dir.rmdir()  # not while a live triggerer still has a directory in it

    def maybe_handle_orphaned_trigger_logs(self, own_job_id: int | None) -> None:
        # At start, and once more when a triggerer that died just before this one started stops counting as
        # alive (a container restarted in place). In a thread: thousands of uploads, each a timeout while the
        # store is unreachable, must not stop this triggerer heartbeating and handling events.
        now = time.monotonic()
        if self._orphan_scans_at is None:
            self._orphan_scans_at = [
                now,
                now + conf.getfloat("triggerer", "triggerer_health_check_threshold"),
            ]
        if not self._orphan_scans_at or now < self._orphan_scans_at[0]:
            return
        if self._orphan_scan_thread is not None and self._orphan_scan_thread.is_alive():
            return  # still uploading what the previous scan found
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
