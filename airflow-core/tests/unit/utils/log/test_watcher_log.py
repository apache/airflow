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

import copy
import errno
import itertools
import json
import logging
import os
import shutil
import stat
import time
from pathlib import Path
from unittest import mock

import attrs
import pytest
from structlog.processors import JSONRenderer

from airflow.jobs.triggerer_job_runner import WatcherLoggingFactory
from airflow.utils.log.trigger_log import get_trigger_log_dir, get_trigger_log_relative_path
from airflow.utils.log.watcher_log import (
    CappedBytesLogger,
    WatcherLogs,
    get_watcher_log_max_open_files,
    handler_keeps_no_copy,
    upload_watcher_log_file,
)

from tests_common.test_utils.config import conf_vars


class WatcherLogFileHelpers:
    """Shared with the watcher log tests that need a triggerer supervisor, in ``unit/jobs/test_triggerer_job.py``."""

    @staticmethod
    def _write(tmp_path, path, text="line\n", mtime=None):
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_text(text)
        if mtime is not None:
            os.utime(tmp_path / path, (mtime, mtime))

    @staticmethod
    def _processors():
        return [JSONRenderer(serializer=lambda obj, **_: json.dumps(obj).encode())]

    class _Store:
        """
        A remote handler that stores files: by default as S3, GCS and WASB do (appending downloads the object and
        uploads all of it again, then the local file is emptied), with ``replace`` as HDFS does (the object is
        replaced, the local file kept), with ``keep`` as OSS does (appended to, the local file kept).
        """

        processors = ()

        def __init__(self, base, *, replace=False, keep=False, delete_local_copy=False):
            self.base_log_folder, self.replace, self.keep = base, replace, keep
            self.down, self.attempts = False, 0
            self.delete_local_copy = delete_local_copy  # as delete_local_logs: remove the file's directory
            self.quiet_failures = (
                0  # uploads that leave the file as it was, as S3, GCS and WASB on a failed write
            )
            self.raised_failures = 0  # uploads that raise, as HDFS on a failed write
            self.objects: dict[str, str] = {}
            self.uploads: list[tuple[str, int, int]] = []  # path, bytes of the file, bytes downloaded
            self.replaced: list[str] = []  # objects whose content was replaced by other content
            self.store = self  # also in a copy, as a real handler's copy writes to the same store

        def upload(self, path, ti):
            store = self.store
            store.attempts += 1
            if store.down:
                raise OSError("store unreachable")
            if store.raised_failures:
                store.raised_failures -= 1
                raise OSError("store unreachable")
            if store.quiet_failures:
                store.quiet_failures -= 1
                return
            local = self.base_log_folder / path
            if not local.is_file():
                return
            new = local.read_text()
            old = self.objects.get(path, "")
            self.uploads.append((path, len(new), 0 if self.replace else len(old)))
            if self.replace:
                if old and old != new:
                    self.replaced.append(path)
                self.objects[path] = new
            else:
                self.objects[path] = old + new
                if not self.keep and not self.delete_local_copy:
                    local.write_text("")
            if self.delete_local_copy:
                shutil.rmtree(local.parent)

        def lines(self):
            return [
                json.loads(line)["n"]
                for key in sorted(self.objects)
                for line in self.objects[key].splitlines()
            ]

    class S3RemoteLogIO(_Store):
        pass

    class GCSRemoteLogIO(_Store):
        pass

    class WasbRemoteLogIO(_Store):
        pass

    class HdfsRemoteLogIO(_Store):
        def __init__(self, base, **kwargs):
            super().__init__(base, replace=True, **kwargs)

    class CustomRemoteLogIO(_Store):
        def __init__(self, base):
            super().__init__(base, keep=True)

    class OSSRemoteLogIO(_Store):
        """As OSS: ``upload`` drops the result of ``oss_write``, which returns False on a failed write."""

        def __init__(self, base, **kwargs):
            super().__init__(base, keep=True, **kwargs)

        def upload(self, path, ti):
            local = self.base_log_folder / path
            if local.is_file() and self.oss_write(local.read_text(), str(path)) and self.delete_local_copy:
                shutil.rmtree(local.parent)

        def oss_write(self, log, remote_log_location, append=True):
            store = self.store
            store.attempts += 1
            if store.raised_failures:  # raised before anything is stored, as when the file can't be read
                store.raised_failures -= 1
                raise OSError("store unreachable")
            if store.down or store.quiet_failures:
                store.quiet_failures = max(0, store.quiet_failures - 1)
                return False
            old = self.objects.get(remote_log_location, "")
            self.uploads.append((remote_log_location, len(log), len(old)))
            self.objects[remote_log_location] = old + log
            return True

    class OldS3RemoteLogIO(S3RemoteLogIO):
        """As S3, GCS and WASB before amazon 9.30.0, google 22.6.0 and microsoft-azure 14.1.0: a stored file is kept."""

        def upload(self, path, ti):
            local = self.base_log_folder / path
            if local.is_file() and self.write(local.read_text(), str(path)) and self.delete_local_copy:
                shutil.rmtree(local.parent)

        def write(self, log, remote_log_location):
            store = self.store
            store.attempts += 1
            if store.down or store.quiet_failures:
                store.quiet_failures = max(0, store.quiet_failures - 1)
                return False
            old = self.objects.get(remote_log_location, "")
            self.uploads.append((remote_log_location, len(log), len(old)))
            self.objects[remote_log_location] = old + log
            return True


class TestWatcherLogFile(WatcherLogFileHelpers):
    @staticmethod
    def _factory(max_bytes=0, **kwargs):
        return WatcherLoggingFactory(
            log_path=get_trigger_log_relative_path(1, 5),
            trigger_id=1,
            max_bytes=max_bytes,
            to_stdout=False,
            **kwargs,
        )

    @pytest.mark.parametrize("max_bytes", [0, 400])
    def test_file_is_appended_to_when_reopened(self, tmp_path, max_bytes):
        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            for run in range(2):
                factory = self._factory(max_bytes)
                factory(self._processors()).log(logging.INFO, "line", run=run)
                factory.close()

        lines = (tmp_path / get_trigger_log_relative_path(1, 5)).read_text().splitlines()
        assert [json.loads(line)["run"] for line in lines] == [0, 1]
        assert {json.loads(line)["trigger_id"] for line in lines} == {1}

    def test_a_closed_file_is_open_again_before_the_processors_see_the_next_line(self, tmp_path):
        # A remote handler that streams each line (CloudWatch) names its stream after the open file.
        seen_closed = []

        def record(logger, method_name, event_dict):
            seen_closed.append(logger._file.closed)
            return event_dict

        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            factory = self._factory()
            factory([record, *self._processors()]).log(logging.INFO, "line", n=1)
            factory.close()
            factory([record, *self._processors()]).log(logging.INFO, "line", n=2)
            factory.close()

        assert seen_closed == [False, False]
        lines = (tmp_path / get_trigger_log_relative_path(1, 5)).read_text().splitlines()
        assert [json.loads(line)["n"] for line in lines] == [1, 2]

    def test_a_line_looks_at_its_file_once(self, tmp_path, mocker):
        # For every watcher line, on the supervisor's main loop.
        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            factory = self._factory()
            factory(self._processors()).log(logging.INFO, "line", n=0)
            fstat = mocker.spy(os, "fstat")
            for n in range(1, 4):
                factory(self._processors()).log(logging.INFO, "line", n=n)
            factory.close()

        assert fstat.call_count == 3

    def test_a_line_a_full_disk_cut_is_removed_before_the_next_line(self, tmp_path):
        # The file is reopened by the next line, which must not be glued to the cut one.
        path = tmp_path / "trigger.log"
        logger = CappedBytesLogger(path, max_bytes=0)
        logger.msg(b'{"n": 0}')
        write = logger._write

        def write_half(data):
            write(data[: len(data) // 2])
            logger._file.flush()
            raise OSError(errno.ENOSPC, "No space left on device")

        logger._write = write_half
        with pytest.raises(OSError, match="No space left"):
            logger.msg(b'{"n": 1}')
        logger.msg(b'{"n": 2}')
        logger.close()

        assert path.read_bytes() == b'{"n": 0}\n{"n": 2}\n'

    @pytest.mark.parametrize("max_bytes", [0, 80], ids=["no-cap", "rotates"])
    @pytest.mark.parametrize("closed", [True, False], ids=["closed", "open"])
    def test_a_removed_directory_is_recreated_by_the_next_line(self, tmp_path, max_bytes, closed):
        # Open: on a shared log folder, another triggerer's orphan scan can remove a running one's files.
        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            factory = self._factory(max_bytes)
            logger = factory(self._processors())
            logger.log(logging.INFO, "line", n=1)
            if closed:
                factory.close()
            shutil.rmtree(tmp_path / get_trigger_log_dir(1))
            logger.log(logging.INFO, "line", n=2)
            logger.log(logging.INFO, "line", n=3)
            factory.close()

        files = [
            tmp_path / f"{get_trigger_log_dir(1, 5)}/rotated/trigger.log",
            tmp_path / get_trigger_log_relative_path(1, 5),
        ]
        lines = [line for f in files if f.exists() for line in f.read_text().splitlines()]
        assert [json.loads(line)["n"] for line in lines] == [2, 3]

    def test_local_disk_is_capped_with_one_rotated_file_in_its_own_directory(self, tmp_path):
        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            factory = self._factory(1000)
            logger = factory(self._processors())
            for n in range(500):
                logger.log(logging.INFO, "line", n=n)
            factory.close()

        live = tmp_path / get_trigger_log_relative_path(1, 5)
        rotated = tmp_path / f"{get_trigger_log_dir(1, 5)}/rotated/trigger.log"
        numbers = [json.loads(line)["n"] for p in (rotated, live) for line in p.read_text().splitlines()]
        assert live.stat().st_size < 1000
        assert rotated.stat().st_size < 1000 + 100
        assert numbers == list(range(numbers[0], 500))

    def test_upload_closes_first_and_sends_the_rotated_file_before_the_live_file(self, tmp_path, mocker):
        # Stores files, not streaming each line.
        handler = mocker.Mock(spec=["processors", "upload"], processors=())
        open_at_upload = []
        handler.upload.side_effect = lambda path, ti: open_at_upload.append(
            not factory.bound_logger._logger._file.closed
        )
        mocker.patch("airflow.sdk.log.load_remote_log_handler", return_value=handler)
        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            factory = self._factory(200)
            logger = factory(self._processors())
            for n in itertools.count():
                logger.log(logging.INFO, "line", n=n)
                if (tmp_path / f"{get_trigger_log_dir(1, 5)}/rotated/trigger.log").exists():
                    break
            logger.log(logging.INFO, "line", n=n + 1)  # a store gets only files with lines
            factory.upload_to_remote()

        assert [c.args for c in handler.upload.call_args_list] == [
            (f"{get_trigger_log_dir(1, 5)}/rotated/trigger.log", None),
            (get_trigger_log_relative_path(1, 5), None),
        ]
        assert open_at_upload == [False, False]

    @pytest.mark.parametrize(
        ("module", "class_name", "kwargs"),
        [
            ("amazon.aws.log.s3_task_handler", "S3RemoteLogIO", {"acl_policy": "bucket-owner-full-control"}),
            ("google.cloud.log.gcs_task_handler", "GCSRemoteLogIO", {"project_id": "project"}),
            ("microsoft.azure.log.wasb_task_handler", "WasbRemoteLogIO", {"wasb_container": "logs"}),
            ("apache.hdfs.log.hdfs_task_handler", "HdfsRemoteLogIO", {}),
            ("alibaba.cloud.log.oss_task_handler", "OSSRemoteLogIO", {}),
        ],
    )
    def test_a_store_removes_only_the_file_it_stored_with_delete_local_logs(
        self, mocker, tmp_path, module, class_name, kwargs
    ):
        module = pytest.importorskip(f"airflow.providers.{module}")
        io_class = getattr(module, class_name)
        handler = io_class(remote_base="logs", base_log_folder=tmp_path, delete_local_copy=True, **kwargs)
        if class_name == "HdfsRemoteLogIO":
            hooks = mocker.patch.object(module, "WebHDFSHook", autospec=True)
        else:
            write = "oss_write" if class_name == "OSSRemoteLogIO" else "write"
            writes = mocker.patch.object(io_class, write, autospec=True, return_value=True)
        copies = mocker.spy(copy, "copy")
        job_dir = get_trigger_log_dir(1, 5)
        part, held, live = (
            f"{job_dir}/{name}" for name in ("rotated/trigger.000001.log", "trigger.held.log", "trigger.log")
        )
        for path in (part, held, live):
            self._write(tmp_path, path, '{"n": 0}\n')

        for path in (live, part):
            upload_watcher_log_file(handler, path, tmp_path / path)

        assert [p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*") if p.is_file()] == [held]
        assert not (tmp_path / job_dir / "rotated").exists()
        # One copy with the handler's settings but where it uploads from; one hook and one client for every upload.
        copies.assert_called_once_with(handler)
        storing = copies.spy_return
        assert attrs.asdict(storing, recurse=False) == {
            **attrs.asdict(handler, recurse=False),
            "base_log_folder": storing.base_log_folder,
            "delete_local_copy": True,
        }
        if class_name == "HdfsRemoteLogIO":
            hooks.assert_called_once()
        else:
            assert [c.args[0] for c in writes.call_args_list] == [storing, storing]

    @pytest.mark.parametrize("stored", [True, False])
    @pytest.mark.parametrize(
        ("module", "class_name", "kwargs"),
        [
            ("amazon.aws.log.s3_task_handler", "S3RemoteLogIO", {}),
            ("google.cloud.log.gcs_task_handler", "GCSRemoteLogIO", {"project_id": "project"}),
            ("microsoft.azure.log.wasb_task_handler", "WasbRemoteLogIO", {"wasb_container": "logs"}),
            ("alibaba.cloud.log.oss_task_handler", "OSSRemoteLogIO", {}),
        ],
    )
    def test_a_store_that_keeps_the_file_it_stored_gets_it_once(
        self, mocker, tmp_path, module, class_name, kwargs, stored
    ):
        # Older releases of these handlers keep the file they stored, and all of them only log a failed write.
        module = pytest.importorskip(f"airflow.providers.{module}")
        io_class = getattr(module, class_name)
        handler = io_class(remote_base="logs", base_log_folder=tmp_path, delete_local_copy=False, **kwargs)
        mocker.patch.object(io_class, "upload", autospec=True, side_effect=self._old_upload)
        write = "oss_write" if class_name == "OSSRemoteLogIO" else "write"
        writes = mocker.patch.object(io_class, write, autospec=True, return_value=stored)
        path = f"{get_trigger_log_dir(1, 5)}/rotated/trigger.000001.log"
        self._write(tmp_path, path, '{"n": 0}\n')

        upload_watcher_log_file(handler, path, tmp_path / path)

        assert writes.call_count == 1
        assert (tmp_path / path).read_text() == ("" if stored else '{"n": 0}\n')
        assert [p.name for p in (tmp_path / path).parent.iterdir()] == ["trigger.000001.log"]

    @staticmethod
    def _old_upload(handler, path, ti=None):
        """``upload`` of these handlers before they emptied a file they stored."""
        local = handler.base_log_folder / path
        if local.is_file():
            write = getattr(handler, "oss_write", None) or handler.write
            if write(local.read_text(), str(path)) and handler.delete_local_copy:
                shutil.rmtree(local.parent)

    @pytest.mark.parametrize(
        ("module", "class_name"),
        [
            ("elasticsearch.log.es_task_handler", "ElasticsearchRemoteLogIO"),
            ("opensearch.log.os_task_handler", "OpensearchRemoteLogIO"),
        ],
    )
    def test_a_store_of_task_logs_only_keeps_no_copy(self, tmp_path, module, class_name):
        module = pytest.importorskip(f"airflow.providers.{module}")
        handler = getattr(module, class_name)(base_log_folder=tmp_path)

        assert handler_keeps_no_copy(handler)

    @pytest.mark.parametrize(
        ("stored", "delete_local_copy"),
        [(True, False), (False, False), (True, True), (False, True)],
    )
    def test_an_appending_store_empties_a_file_only_once_stored(self, tmp_path, stored, delete_local_copy):
        # OSS reports a failed write only through oss_write's return value: a file kept is sent again.
        store = self.OSSRemoteLogIO(tmp_path, delete_local_copy=delete_local_copy)
        store.quiet_failures = int(not stored)
        relative_path = "triggers/1/2/rotated/trigger.000001.log"
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True)
        path.write_text('{"n": 0}\n')

        upload_watcher_log_file(store, relative_path, path)
        assert store.lines() == ([0] if stored else [])
        if stored:
            assert not path.parent.exists() if delete_local_copy else path.read_text() == ""
        else:
            assert path.read_text() == '{"n": 0}\n'

        upload_watcher_log_file(store, relative_path, path)
        assert store.lines() == [0]

    def test_a_rotation_that_fails_leaves_the_line_written(self, tmp_path, mocker):
        # E.g. no thread left for the pool: raised out of the line, it stops the reading of the runner's logs.
        watcher_logger = mocker.patch("airflow.utils.log.watcher_log.log")
        triggerer_log = mocker.patch("airflow.jobs.triggerer_job_runner.log")
        on_rotate = mocker.Mock(side_effect=RuntimeError("can't start new thread"))
        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            factory = self._factory(200, on_rotate=on_rotate)
            logger = factory(self._processors())
            n = 0
            while not on_rotate.called:
                logger.log(logging.INFO, "line", n=n)
                n += 1
            logger.log(logging.INFO, "line", n=n)
            factory.close()

        files = [
            tmp_path / f"{get_trigger_log_dir(1, 5)}/rotated/trigger.log",
            tmp_path / get_trigger_log_relative_path(1, 5),
        ]
        numbers = [json.loads(line)["n"] for f in files for line in f.read_text().splitlines()]
        assert numbers == list(range(n + 1))
        assert on_rotate.call_count == 1
        assert watcher_logger.exception.call_count == 1
        triggerer_log.log.assert_not_called()  # not sent to the triggerer's output as a line the file lost

    def test_a_rotation_whose_new_file_cannot_be_opened_still_sends_the_rotated_lines(self, tmp_path, mocker):
        # E.g. no file descriptor left: the next rotation would replace them.
        mocker.patch("airflow.jobs.triggerer_job_runner.log")
        rotated = tmp_path / f"{get_trigger_log_dir(1, 5)}/rotated/trigger.log"
        sent = []
        touch, failures = Path.touch, [OSError(errno.EMFILE, "Too many open files")]

        def touch_once_failing(path, *args, **kwargs):
            if failures:
                raise failures.pop()
            return touch(path, *args, **kwargs)

        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            factory = self._factory(200, on_rotate=lambda: sent.append(rotated.read_text()))
            factory(self._processors())
            mocker.patch.object(Path, "touch", autospec=True, side_effect=touch_once_failing)
            for n in range(40):
                factory(self._processors()).log(logging.INFO, "line", n=n)
            factory.close()

        live = (tmp_path / get_trigger_log_relative_path(1, 5)).read_text()
        assert not failures
        assert len(sent) > 2
        assert [json.loads(line)["n"] for text in [*sent, live] for line in text.splitlines()] == list(
            range(40)
        )

    @pytest.mark.parametrize(
        ("setting", "soft_limit", "expected"),
        [
            pytest.param(256, 4096, 256, id="setting-below-a-quarter-of-the-limit"),
            pytest.param(256, 256, 64, id="limit-as-low-as-the-setting"),
            pytest.param(0, 512, 128, id="no-limit-set"),
        ],
    )
    def test_open_files_stay_below_the_processs_limit(self, mocker, setting, soft_limit, expected):
        mocker.patch("resource.getrlimit", return_value=(soft_limit, soft_limit))
        with conf_vars({("triggerer", "watcher_log_max_open_files"): str(setting)}):
            assert get_watcher_log_max_open_files() == expected

    @conf_vars(
        {
            ("logging", "file_task_handler_new_folder_permissions"): "0o700",
            ("logging", "file_task_handler_new_file_permissions"): "0o600",
        }
    )
    def test_a_file_recreated_after_an_upload_gets_the_log_permissions(self, tmp_path):
        with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
            factory = self._factory(80)
            logger = factory(self._processors())
            logger.log(logging.INFO, "line", n=1)
            shutil.rmtree(tmp_path / get_trigger_log_dir(1, 5))  # as delete_local_logs does after an upload
            for n in range(2, 6):
                logger.log(logging.INFO, "line", n=n)
            factory.close()

        job_dir = tmp_path / get_trigger_log_dir(1, 5)
        assert (tmp_path / f"{get_trigger_log_dir(1, 5)}/rotated/trigger.log").exists()
        assert [
            stat.S_IMODE(p.stat().st_mode) for p in (job_dir, job_dir / "rotated", job_dir / "trigger.log")
        ] == [0o700, 0o700, 0o600]

    def test_a_stale_claim_is_taken_over_by_one_of_two_scans(self, tmp_path):
        # Two triggerers on a shared log folder both find a dead one's stale claim. The second looked at it
        # before the first took it over, and goes on to take it over while the first is uploading.
        dead_id = 5
        self._write(tmp_path, get_trigger_log_relative_path(1, dead_id), '{"n": 0}\n')
        producer_dir = tmp_path / get_trigger_log_dir(1, dead_id)
        claim = producer_dir / ".uploading"
        self._write(tmp_path, claim.relative_to(tmp_path), "", mtime=time.time() - 3600)
        stale, real_stat = claim.stat(), Path.stat
        store, second, looked, held = self.S3RemoteLogIO(tmp_path), [], [], []

        def stat(p, **kwargs):
            if p == claim and not looked:
                looked.append(p)
                return stale  # as when the second scan looked at it
            return real_stat(p, **kwargs)

        def upload(path, ti):
            if not looked:  # the first scan's upload: the second scan runs meanwhile
                with mock.patch.object(Path, "stat", stat):
                    second.append(
                        WatcherLogs._upload_orphaned_trigger_log_dir(store, tmp_path, producer_dir, dead_id)
                    )
                held.append(claim.exists())  # still the first scan's
            type(store).upload(store, path, ti)

        with mock.patch.object(store, "upload", upload):
            first = WatcherLogs._upload_orphaned_trigger_log_dir(store, tmp_path, producer_dir, dead_id)

        assert (first, second, held) == (False, [None], [True])
        assert [path for path, _, _ in store.uploads] == [get_trigger_log_relative_path(1, dead_id)]
        assert not list(tmp_path.glob("triggers/*/*/.uploading*"))
