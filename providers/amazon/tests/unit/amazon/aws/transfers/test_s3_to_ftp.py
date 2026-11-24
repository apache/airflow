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

import ftplib
from unittest import mock

import pytest

from airflow.providers.amazon.aws.transfers.s3_to_ftp import S3ToFTPOperator
from airflow.providers.amazon.aws.utils import validate_destination_path

TASK_ID = "test_s3_to_ftp"
BUCKET = "test-s3-bucket"
S3_KEY = "test/test_1_file.csv"
FTP_PATH = "/tmp/remote_path.txt"
AWS_CONN_ID = "aws_default"
FTP_CONN_ID = "ftp_default"


class TestS3ToFTPOperator:
    @mock.patch("airflow.providers.ftp.hooks.ftp.FTPHook.store_file")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_key")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.check_for_key", return_value=True)
    @mock.patch("airflow.providers.amazon.aws.transfers.s3_to_ftp.NamedTemporaryFile")
    def test_execute(
        self, mock_local_tmp_file, mock_check_for_key, mock_s3_hook_get_key, mock_ftp_hook_store_file
    ):
        operator = S3ToFTPOperator(task_id=TASK_ID, s3_bucket=BUCKET, s3_key=S3_KEY, ftp_path=FTP_PATH)
        operator.execute(None)

        mock_s3_hook_get_key.assert_called_once_with(operator.s3_key, operator.s3_bucket)

        mock_local_tmp_file_value = mock_local_tmp_file.return_value.__enter__.return_value
        mock_s3_hook_get_key.return_value.download_fileobj.assert_called_once_with(mock_local_tmp_file_value)
        mock_ftp_hook_store_file.assert_called_once_with(operator.ftp_path, mock_local_tmp_file_value.name)

    @pytest.mark.parametrize(
        ("keys", "expected", "skipped"),
        [
            pytest.param(
                ["source/pre_one.txt", "source/xpre_two.txt", "source/pre_again_pre_.txt"],
                [
                    ("source/pre_one.txt", "/destination/new_one.txt"),
                    ("source/pre_again_pre_.txt", "/destination/new_again_pre_.txt"),
                ],
                ["xpre_two.txt"],
                id="prefix-only-at-start",
            ),
            pytest.param(
                ["source/pre_one.txt", "source/pre_dir/pre_two.txt", "source/pre_dir/other.txt"],
                [
                    ("source/pre_one.txt", "/destination/new_one.txt"),
                    ("source/pre_dir/pre_two.txt", "/destination/pre_dir/new_two.txt"),
                ],
                ["pre_dir/other.txt"],
                id="nested-keys-keep-their-directory",
            ),
            pytest.param(
                ["source/pre_dir/", "source/pre_dir/pre_two.txt", "source/pre_dir/other.txt"],
                [("source/pre_dir/pre_two.txt", "/destination/pre_dir/new_two.txt")],
                ["pre_dir/other.txt"],
                id="folder-marker-is-not-selected-or-reported",
            ),
        ],
    )
    @mock.patch.object(S3ToFTPOperator, "_download_from_s3")
    @mock.patch("airflow.providers.amazon.aws.transfers.s3_to_ftp.FTPHook")
    @mock.patch("airflow.providers.amazon.aws.transfers.s3_to_ftp.S3Hook")
    def test_execute_matches_the_prefix_on_the_file_name(
        self, mock_s3_hook_class, mock_ftp_hook_class, mock_download_from_s3, keys, expected, skipped
    ):
        """The prefix matches only the start of the file name, and only that leading occurrence is
        renamed. ``list_keys`` recurses, so a nested key is matched on its file name and keeps its
        directory, while the old substring rule still decides which skipped keys are reported. A
        folder marker has no file name, so it is neither selected, created nor reported."""
        mock_s3_hook = mock_s3_hook_class.return_value
        mock_s3_hook.list_keys.return_value = keys
        operator = S3ToFTPOperator(
            task_id=TASK_ID,
            s3_bucket=BUCKET,
            s3_key="source/",
            ftp_path="/destination/",
            s3_filenames="pre_",
            ftp_filenames="new_",
        )

        with mock.patch.object(operator.log, "warning") as mock_log_warning:
            operator.execute(None)

        assert mock_download_from_s3.call_args_list == [
            mock.call(mock_s3_hook, mock_ftp_hook_class.return_value, *call) for call in expected
        ]
        mock_log_warning.assert_called_once_with(mock.ANY, len(skipped), "pre_", skipped, "")
        mock_ftp_hook_class.return_value.create_directory.assert_not_called()

    @staticmethod
    def _fake_ftp_server(ftp_hook, *, dirs, files, events):
        """Back the mocked hook with in-memory paths. Like a real server, CWD succeeds only for a
        directory and MKD answers 550 for any existing path. Returns the session state."""
        conn = ftp_hook.get_conn.return_value
        state = {"cwd": "/home/user"}

        def fake_cwd(path):
            if path not in dirs:
                raise ftplib.error_perm(f"550 {path}: No such file or directory")
            state["cwd"] = path

        def fake_mkd(path):
            if path in dirs or path in files:
                raise ftplib.error_perm(f"550 {path}: File exists")
            dirs.add(path)
            events.append(("mkd", path))

        conn.pwd.side_effect = lambda: state["cwd"]
        conn.cwd.side_effect = fake_cwd
        ftp_hook.create_directory.side_effect = fake_mkd
        return state

    @mock.patch.object(S3ToFTPOperator, "_download_from_s3")
    @mock.patch("airflow.providers.amazon.aws.transfers.s3_to_ftp.FTPHook")
    @mock.patch("airflow.providers.amazon.aws.transfers.s3_to_ftp.S3Hook")
    def test_execute_creates_folder_markers_as_directories(
        self, mock_s3_hook_class, mock_ftp_hook_class, mock_download_from_s3
    ):
        """A key ending in ``/`` is a folder marker, such as the S3 console creates for each folder.
        With ``"*"`` each marker is created as a directory, with any missing parents, before any file
        is transferred, so a file under a marker lands in it. A directory that exists is kept, and
        the session returns to its working directory."""
        events: list[tuple[str, ...]] = []
        dirs = {"/", "/home/user", "/destination"}
        state = self._fake_ftp_server(mock_ftp_hook_class.return_value, dirs=dirs, files=set(), events=events)
        mock_download_from_s3.side_effect = lambda s3, ftp, key, path: events.append(("put", key, path))
        mock_s3_hook_class.return_value.list_keys.return_value = [
            "source/",
            "source/deep/nested/",
            "source/empty/",
            "source/one.txt",
            "source/sub/",
            "source/sub/two.txt",
        ]
        operator = S3ToFTPOperator(
            task_id=TASK_ID,
            s3_bucket=BUCKET,
            s3_key="source/",
            ftp_path="/destination/",
            s3_filenames="*",
        )

        operator.execute(None)

        assert events == [
            ("mkd", "/destination/deep"),
            ("mkd", "/destination/deep/nested"),
            ("mkd", "/destination/empty"),
            ("mkd", "/destination/sub"),
            ("put", "source/one.txt", "/destination/one.txt"),
            ("put", "source/sub/two.txt", "/destination/sub/two.txt"),
        ]
        assert state["cwd"] == "/home/user"

    @pytest.mark.parametrize(
        ("keys", "files", "error", "match"),
        [
            pytest.param(
                ["source/", "source/sub/"],
                {"/destination/sub"},
                ftplib.error_perm,
                "File exists",
                id="file-at-marker-path",
            ),
            pytest.param(
                ["source/../../etc/"],
                set(),
                ValueError,
                "escapes configured ftp_path",
                id="marker-escapes-ftp-path",
            ),
        ],
    )
    @mock.patch.object(S3ToFTPOperator, "_download_from_s3")
    @mock.patch("airflow.providers.amazon.aws.transfers.s3_to_ftp.FTPHook")
    @mock.patch("airflow.providers.amazon.aws.transfers.s3_to_ftp.S3Hook")
    def test_execute_fails_on_folder_marker_that_cannot_be_a_directory(
        self, mock_s3_hook_class, mock_ftp_hook_class, mock_download_from_s3, keys, files, error, match
    ):
        events: list[tuple[str, ...]] = []
        dirs = {"/", "/home/user", "/destination"}
        self._fake_ftp_server(mock_ftp_hook_class.return_value, dirs=dirs, files=files, events=events)
        mock_s3_hook_class.return_value.list_keys.return_value = keys
        operator = S3ToFTPOperator(
            task_id=TASK_ID,
            s3_bucket=BUCKET,
            s3_key="source/",
            ftp_path="/destination/",
            s3_filenames="*",
        )

        with pytest.raises(error, match=match):
            operator.execute(None)

        assert events == []
        mock_download_from_s3.assert_not_called()


class TestS3ToFTPOperatorInit:
    """Unit tests for S3ToFTPOperator.__init__ that do not require an FTP server."""

    @pytest.mark.parametrize(
        ("s3_filenames", "ftp_filenames"),
        [
            (None, None),
            ("*", None),
            ("prefix_", "renamed_"),
            (["a.csv", "b.csv"], ["x.csv", "y.csv"]),
        ],
    )
    def test_multi_file_params(self, s3_filenames, ftp_filenames):
        """s3_filenames and ftp_filenames are stored correctly."""
        op = S3ToFTPOperator(
            task_id="test_multi",
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            ftp_path=FTP_PATH,
            s3_filenames=s3_filenames,
            ftp_filenames=ftp_filenames,
        )
        assert op.s3_filenames == s3_filenames
        assert op.ftp_filenames == ftp_filenames

    def test_fail_on_file_not_exist_default(self):
        """fail_on_file_not_exist defaults to True."""
        op = S3ToFTPOperator(task_id="test_fail_default", s3_bucket=BUCKET, s3_key=S3_KEY, ftp_path=FTP_PATH)
        assert op.fail_on_file_not_exist is True

    @pytest.mark.parametrize("fail_on_file_not_exist", [True, False])
    def test_fail_on_file_not_exist_skip(self, fail_on_file_not_exist):
        """When key is missing: raise FileNotFoundError if True, skip if False."""
        from unittest.mock import MagicMock, patch

        op = S3ToFTPOperator(
            task_id="test_skip",
            s3_bucket=BUCKET,
            s3_key=S3_KEY,
            ftp_path=FTP_PATH,
            fail_on_file_not_exist=fail_on_file_not_exist,
        )
        mock_s3_hook = MagicMock()
        mock_s3_hook.check_for_key.return_value = False

        if fail_on_file_not_exist:
            with pytest.raises(FileNotFoundError):
                op._download_from_s3(mock_s3_hook, MagicMock(), S3_KEY, FTP_PATH)
        else:
            with patch.object(op.log, "info") as mock_log:
                op._download_from_s3(mock_s3_hook, MagicMock(), S3_KEY, FTP_PATH)
            mock_log.assert_called_once()

    @pytest.mark.parametrize(
        "object_suffix",
        [
            pytest.param("../../../../etc/cron.d/evil", id="dotdot-segments"),
            pytest.param("subdir/../../../escape", id="dotdot-cancels-base"),
        ],
    )
    def test_validate_destination_path_rejects_escape(self, object_suffix):
        # In multi-file mode the destination is ``ftp_path`` plus a key suffix
        # returned by ``list_keys``; a crafted object name must not place the
        # upload outside the configured ``ftp_path`` on the FTP server.
        ftp_path = "/srv/ftp/incoming/"
        with pytest.raises(ValueError, match="escapes configured ftp_path"):
            validate_destination_path(ftp_path + object_suffix, ftp_path, base_name="ftp_path")

    def test_validate_destination_path_allows_contained(self):
        ftp_path = "/srv/ftp/incoming/"
        validate_destination_path(ftp_path + "sub/dir/report.csv", ftp_path, base_name="ftp_path")
