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

from pathlib import Path

import pytest

from airflow.providers.apache.ozone.utils.helpers import (
    FileHelper,
    TypeNormalizationHelper,
    URIHelper,
)


def test_normalize_optional_str_strips_and_handles_empty():
    assert TypeNormalizationHelper.normalize_optional_str("  value  ") == "value"
    assert TypeNormalizationHelper.normalize_optional_str("   ") is None
    assert TypeNormalizationHelper.normalize_optional_str(None) is None


def test_require_optional_non_empty_preserves_type_contract():
    assert TypeNormalizationHelper.require_optional_non_empty("  abc ", "err") == "abc"
    assert TypeNormalizationHelper.require_optional_non_empty(None, "err") is None
    with pytest.raises(ValueError, match="err"):
        TypeNormalizationHelper.require_optional_non_empty(123, "err")
    with pytest.raises(ValueError, match="err"):
        TypeNormalizationHelper.require_optional_non_empty("   ", "err")


def test_normalize_flag_bool():
    assert TypeNormalizationHelper.normalize_flag_bool("true", default=False) is True
    assert TypeNormalizationHelper.normalize_flag_bool("No", default=True) is False
    assert TypeNormalizationHelper.normalize_flag_bool(None, default=True) is True


def test_get_file_size_bytes(tmp_path: Path):
    target = tmp_path / "payload.txt"
    target.write_text("hello", encoding="utf-8")
    assert FileHelper.get_file_size_bytes(target) == 5


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("ofs://adho/vol1/bucket1/dir/file.txt", "o3://adho/vol1/bucket1/dir/file.txt"),
        ("o3fs://adho/vol1/bucket1/dir/file.txt", "o3://adho/vol1/bucket1/dir/file.txt"),
        ("o3://adho/vol1/bucket1/dir/file.txt", "o3://adho/vol1/bucket1/dir/file.txt"),
        ("s3a://bucket/key", "s3a://bucket/key"),
        ("vol1/bucket1/dir/file.txt", "vol1/bucket1/dir/file.txt"),
        ("/vol1/bucket1/dir/file.txt", "vol1/bucket1/dir/file.txt"),
    ],
)
def test_to_key_uri_returns_ozone_sh_key_target(value: str, expected: str):
    assert URIHelper.to_key_uri(value) == expected
