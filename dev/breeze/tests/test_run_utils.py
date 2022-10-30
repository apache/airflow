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

import os
import stat
from pathlib import Path
from unittest import TestCase

from airflow_breeze.utils.run_utils import (
    change_directory_permission,
    change_file_permission,
    filter_out_none,
)


def test_change_file_permission(tmpdir):
    tmpfile = Path(tmpdir, "test.config")
    tmpfile.write_text("content")
    change_file_permission(tmpfile)
    mode = os.stat(tmpfile).st_mode
    assert not (mode & stat.S_IWGRP) and not (mode & stat.S_IWOTH)


def test_change_directory_permission(tmpdir):
    change_directory_permission(tmpdir)
    mode = os.stat(tmpdir).st_mode
    assert (
        not (mode & stat.S_IWGRP)
        and not (mode & stat.S_IWOTH)
        and (mode & stat.S_IXGRP)
        and (mode & stat.S_IXOTH)
    )


def test_filter_out_none():
    dict_input_with_none = {"sample": None, "sample1": "One", "sample2": "Two", "samplen": None}
    expected_dict_output = {"sample1": "One", "sample2": "Two"}
    output_dict = filter_out_none(**dict_input_with_none)
    TestCase().assertDictEqual(output_dict, expected_dict_output)
