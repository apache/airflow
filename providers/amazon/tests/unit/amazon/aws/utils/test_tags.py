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

from airflow.providers.amazon.aws.utils.tags import format_tags


@pytest.fixture
def source():
    return {"key1": "value1", "key2": "value2"}


def test_format_tags_with_dict(source):
    formatted_tags = format_tags(source)
    expected_result = [{"Key": "key1", "Value": "value1"}, {"Key": "key2", "Value": "value2"}]
    assert formatted_tags == expected_result


def test_format_tags_with_other_input():
    source = [{"Key": "key1", "Value": "value1"}, {"Key": "key2", "Value": "value2"}]
    formatted_tags = format_tags(source)
    assert formatted_tags == source


def test_format_tags_with_custom_labels(source):
    formatted_tags = format_tags(source, key_label="TagKey", value_label="TagValue")
    expected_result = [{"TagKey": "key1", "TagValue": "value1"}, {"TagKey": "key2", "TagValue": "value2"}]
    assert formatted_tags == expected_result


def test_format_tags_with_none_input():
    formatted_tags = format_tags(None)
    assert formatted_tags == []
