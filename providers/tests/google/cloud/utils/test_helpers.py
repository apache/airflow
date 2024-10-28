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

import pytest

from airflow.providers.google.cloud.utils.helpers import (
    normalize_directory_path,
    resource_path_to_dict,
)


class TestHelpers:
    def test_normalize_directory_path(self):
        assert normalize_directory_path("dir_path") == "dir_path/"
        assert normalize_directory_path("dir_path/") == "dir_path/"
        assert normalize_directory_path(None) is None

    def test_resource_path_to_dict(self):
        resource_name = "key1/value1/key2/value2"
        expected_dict = {"key1": "value1", "key2": "value2"}
        actual_dict = resource_path_to_dict(resource_name=resource_name)
        assert set(actual_dict.items()) == set(expected_dict.items())

    def test_resource_path_to_dict_empty(self):
        resource_name = ""
        expected_dict = {}
        assert resource_path_to_dict(resource_name=resource_name) == expected_dict

    def test_resource_path_to_dict_fail(self):
        with pytest.raises(ValueError):
            resource_path_to_dict(resource_name="key/value/key")
