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

from airflow.api_fastapi.core_api.middleware import RegexpExceptionMiddleware


class TestRegexExceptionMiddleware:
    @pytest.mark.parametrize(
        "data, expected",
        [
            ({"key": "value"}, None),
            ({"key": "value", "key2": "value2"}, None),
            ({"key": "value.*"}, "key"),
            ({"key": "value.*", "key2": "value2"}, "key"),
            ({"key": "value", "key2": "value2.*"}, "key2"),
            ({"key": "value", "key2": "^value2$", "key3": "value3"}, "key2"),
            ({"key": "value", "key2": "^value2$", "key3": "value3.*"}, "key2"),
            ({"key": "value", "key2": {"key3": "value3.*"}}, "key3"),
            ({"key": "value", "key2": {"key3": "^[0-9]*$"}}, "key3"),
            ({"key": "value", "key2": {"key3": "^[0-9]*[a-z]*$"}}, "key3"),
        ],
    )
    def test_detect_regexp_in_dict_values(self, data, expected):
        assert RegexpExceptionMiddleware._detect_regexp(key=None, value=data) == expected
