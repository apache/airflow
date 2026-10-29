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

from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.apache.ozone.hooks.ozone import OzoneFsHook
from airflow.providers.apache.ozone.operators.ozone import (
    OzoneCopyOperator,
    OzoneCreateBucketOperator,
    OzoneCreatePathOperator,
    OzoneCreateVolumeOperator,
    OzoneDeleteBucketOperator,
    OzoneDeleteKeyOperator,
    OzoneDeletePathOperator,
    OzoneDeleteVolumeOperator,
    OzoneDownloadFileOperator,
    OzoneListOperator,
    OzoneMoveOperator,
    OzonePathExistsOperator,
    OzoneSetQuotaOperator,
    OzoneUploadContentOperator,
    OzoneUploadFileOperator,
)


class TestOzoneAdminOperators:
    def test_admin_operator_template_fields_cover_runtime_params(self):
        assert OzoneCreateVolumeOperator.template_fields == ("volume_name", "quota", "ozone_conn_id")
        assert OzoneCreateBucketOperator.template_fields == (
            "volume_name",
            "bucket_name",
            "quota",
            "ozone_conn_id",
        )
        assert OzoneSetQuotaOperator.template_fields == ("volume", "quota", "bucket", "ozone_conn_id")
        assert OzoneDeleteVolumeOperator.template_fields == ("volume_name", "ozone_conn_id")
        assert OzoneDeleteBucketOperator.template_fields == ("volume_name", "bucket_name", "ozone_conn_id")

    @patch("airflow.providers.apache.ozone.operators.ozone.OzoneAdminHook")
    def test_admin_operators_execute_core_flows(self, mock_admin_hook: MagicMock):
        hook = mock_admin_hook.return_value

        OzoneCreateVolumeOperator(task_id="create_volume", volume_name="test_vol").execute(context={})
        OzoneDeleteVolumeOperator(
            task_id="delete_volume",
            volume_name="test_vol",
            recursive=True,
            force=True,
        ).execute(context={})
        OzoneSetQuotaOperator(task_id="set_quota", volume="test_vol", quota="1TB").execute(context={})

        assert mock_admin_hook.call_count == 3
        hook.create_volume.assert_called_once_with("test_vol", None, timeout=3600, if_exists="ignore")
        hook.delete_volume.assert_called_once_with("test_vol", True, True, timeout=3600)
        hook.set_quota.assert_called_once_with(volume="test_vol", quota="1TB", bucket=None, timeout=300)

    def test_admin_operators_validate_force_recursive_contract(self):
        with pytest.raises(ValueError, match="force=True requires recursive=True"):
            OzoneDeleteVolumeOperator(
                task_id="bad_volume_delete", volume_name="v", force=True, recursive=False
            )
        with pytest.raises(ValueError, match="force=True requires recursive=True"):
            OzoneDeleteBucketOperator(
                task_id="bad_bucket_delete",
                volume_name="v",
                bucket_name="b",
                force=True,
                recursive=False,
            )


@pytest.mark.parametrize(
    ("operator", "hook_method", "expected_args", "expected_kwargs", "hook_return"),
    [
        (
            OzoneListOperator(task_id="list_files", path="ofs://vol1/b1/"),
            "list_keys",
            ("ofs://vol1/b1/",),
            {"timeout": 300},
            ["ofs://vol1/b1/f1"],
        ),
        (
            OzoneDeleteKeyOperator(task_id="delete_key", path="ofs://vol1/b1/f1"),
            "delete_key",
            ("ofs://vol1/b1/f1",),
            {"timeout": 3600},
            None,
        ),
        (
            OzoneCreatePathOperator(task_id="create_path", path="ofs://vol1/b1/dir"),
            "make_path",
            ("ofs://vol1/b1/dir",),
            {"timeout": 300, "if_exists": "ignore"},
            None,
        ),
        (
            OzoneDeletePathOperator(task_id="delete_path", path="ofs://vol1/b1/dir", recursive=True),
            "delete_path",
            ("ofs://vol1/b1/dir",),
            {"recursive": True, "timeout": 3600},
            None,
        ),
        (
            OzonePathExistsOperator(task_id="path_exists", path="ofs://vol1/b1/dir"),
            "path_exists",
            ("ofs://vol1/b1/dir",),
            {"timeout": 300},
            True,
        ),
        (
            OzoneCopyOperator(
                task_id="copy_path",
                source_path="ofs://vol1/b1/src.txt",
                dest_path="ofs://vol1/b1/dst.txt",
            ),
            "copy_path",
            ("ofs://vol1/b1/src.txt", "ofs://vol1/b1/dst.txt"),
            {"timeout": 3600, "if_exists": "error"},
            None,
        ),
        (
            OzoneMoveOperator(
                task_id="move_path",
                source_path="ofs://vol1/b1/src.txt",
                dest_path="ofs://vol1/b1/dst.txt",
            ),
            "move",
            ("ofs://vol1/b1/src.txt", "ofs://vol1/b1/dst.txt"),
            {"timeout": 3600, "if_exists": "error"},
            None,
        ),
    ],
)
@patch("airflow.providers.apache.ozone.operators.ozone.OzoneFsHook")
def test_fs_operators_delegate_to_hook(
    mock_ozone_fs_hook: MagicMock,
    operator,
    hook_method: str,
    expected_args: tuple[str, ...],
    expected_kwargs: dict[str, object],
    hook_return,
):
    hook = mock_ozone_fs_hook.return_value
    getattr(hook, hook_method).return_value = hook_return

    result = operator.execute(context={})

    assert mock_ozone_fs_hook.call_args.kwargs["ozone_conn_id"] == OzoneFsHook.default_conn_name
    getattr(hook, hook_method).assert_called_once_with(*expected_args, **expected_kwargs)
    if hook_method in {"list_keys", "path_exists"}:
        assert result == hook_return
    else:
        assert result is None


def test_fs_operator_template_fields_cover_runtime_params():
    assert OzoneCreatePathOperator.template_fields == ("path", "ozone_conn_id")
    assert OzoneDeleteKeyOperator.template_fields == ("path", "ozone_conn_id")
    assert OzoneDeletePathOperator.template_fields == ("path", "ozone_conn_id")
    assert OzonePathExistsOperator.template_fields == ("path", "ozone_conn_id")
    assert OzoneListOperator.template_fields == ("path", "ozone_conn_id")
    assert OzoneUploadContentOperator.template_fields == ("content", "remote_path", "ozone_conn_id")
    assert OzoneUploadFileOperator.template_fields == ("local_path", "remote_path", "ozone_conn_id")
    assert OzoneMoveOperator.template_fields == ("source_path", "dest_path", "ozone_conn_id")
    assert OzoneCopyOperator.template_fields == ("source_path", "dest_path", "ozone_conn_id")
    assert OzoneDownloadFileOperator.template_fields == ("remote_path", "local_path", "ozone_conn_id")


class TestOzoneFileOperators:
    @patch("airflow.providers.apache.ozone.operators.ozone.OzoneFsHook")
    def test_upload_content_enforces_size_limit_and_uploads_when_valid(self, mock_ozone_fs_hook: MagicMock):
        hook = mock_ozone_fs_hook.return_value
        hook.connection_snapshot.max_content_size_bytes = 3
        too_large = OzoneUploadContentOperator(
            task_id="upload_content_too_large",
            content="1234",
            remote_path="ofs://vol1/b1/payload.txt",
        )
        with pytest.raises(AirflowException, match="exceeds configured limit"):
            too_large.execute(context={})
        hook.upload_key.assert_not_called()

        hook.connection_snapshot.max_content_size_bytes = 10
        valid = OzoneUploadContentOperator(
            task_id="upload_content_ok",
            content="1234",
            remote_path="ofs://vol1/b1/payload.txt",
        )
        valid.execute(context={})
        hook.upload_key.assert_called_once()
        assert hook.upload_key.call_args.kwargs["if_exists"] == "error"

    @pytest.mark.parametrize(
        ("is_readable", "file_size", "expected_error"),
        [
            (False, 1, "Local file not found or is not readable"),
            (True, -1, "size is unavailable or non-positive"),
            (True, 0, "size is unavailable or non-positive"),
            (True, 1025, "exceeds configured limit"),
        ],
    )
    @patch("airflow.providers.apache.ozone.operators.ozone.OzoneFsHook")
    @patch("airflow.providers.apache.ozone.operators.ozone.FileHelper.get_file_size_bytes")
    @patch("airflow.providers.apache.ozone.operators.ozone.FileHelper.is_readable_file")
    def test_upload_file_guard_paths(
        self,
        mock_is_readable_file: MagicMock,
        mock_get_file_size_bytes: MagicMock,
        mock_ozone_fs_hook: MagicMock,
        is_readable: bool,
        file_size: int,
        expected_error: str,
    ):
        hook = mock_ozone_fs_hook.return_value
        hook.connection_snapshot.max_content_size_bytes = 1024
        mock_is_readable_file.return_value = is_readable
        mock_get_file_size_bytes.return_value = file_size

        operator = OzoneUploadFileOperator(
            task_id="upload_file_guard",
            local_path="/tmp/src.txt",
            remote_path="ofs://vol1/b1/src.txt",
        )
        with pytest.raises(AirflowException, match=expected_error):
            operator.execute(context={})
        hook.upload_key.assert_not_called()

    @patch("airflow.providers.apache.ozone.operators.ozone.OzoneFsHook")
    @patch("airflow.providers.apache.ozone.operators.ozone.FileHelper.get_file_size_bytes")
    @patch("airflow.providers.apache.ozone.operators.ozone.FileHelper.is_readable_file")
    def test_upload_file_success(
        self,
        mock_is_readable_file: MagicMock,
        mock_get_file_size_bytes: MagicMock,
        mock_ozone_fs_hook: MagicMock,
    ):
        hook = mock_ozone_fs_hook.return_value
        hook.connection_snapshot.max_content_size_bytes = 2048
        mock_is_readable_file.return_value = True
        mock_get_file_size_bytes.return_value = 1024

        operator = OzoneUploadFileOperator(
            task_id="upload_file_ok",
            local_path="/tmp/src.txt",
            remote_path="ofs://vol1/b1/src.txt",
        )
        operator.execute(context={})

        hook.upload_key.assert_called_once_with(
            "/tmp/src.txt",
            "ofs://vol1/b1/src.txt",
            timeout=3600,
            if_exists="error",
        )

    @pytest.mark.parametrize(
        ("data_size", "expected_error"),
        [
            (None, "Unable to determine remote key size"),
            ("invalid", "Unable to determine remote key size"),
            (101, "exceeds configured limit"),
        ],
    )
    @patch("airflow.providers.apache.ozone.operators.ozone.OzoneFsHook")
    def test_download_file_guard_paths(
        self,
        mock_ozone_fs_hook: MagicMock,
        data_size,
        expected_error: str,
    ):
        hook = mock_ozone_fs_hook.return_value
        hook.connection_snapshot.max_content_size_bytes = 100
        hook.get_key_property.return_value = {"data_size": data_size}
        operator = OzoneDownloadFileOperator(
            task_id="download_file_guard",
            remote_path="ofs://vol1/b1/src.txt",
            local_path="/tmp/dst.txt",
        )
        with pytest.raises(AirflowException, match=expected_error):
            operator.execute(context={})
        hook.download_key.assert_not_called()

    @patch("airflow.providers.apache.ozone.operators.ozone.OzoneFsHook")
    def test_download_file_success(self, mock_ozone_fs_hook: MagicMock):
        hook = mock_ozone_fs_hook.return_value
        hook.connection_snapshot.max_content_size_bytes = 1024 * 1024
        hook.get_key_property.return_value = {"data_size": 100}
        operator = OzoneDownloadFileOperator(
            task_id="download_file_ok",
            remote_path="ofs://vol1/b1/src.txt",
            local_path="/tmp/dst.txt",
            overwrite=True,
        )
        operator.execute(context={})
        hook.get_key_property.assert_called_once_with("ofs://vol1/b1/src.txt", timeout=operator.timeout)
        hook.download_key.assert_called_once_with(
            "ofs://vol1/b1/src.txt",
            "/tmp/dst.txt",
            overwrite=True,
            timeout=operator.timeout,
        )
