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

import jsonpath_ng
import pytest

from airflow.providers.amazon.aws.utils.sqs import (
    filter_messages,
    filter_messages_jsonpath,
    filter_messages_literal,
    process_response,
)


@pytest.fixture
def response_json():
    return {"Messages": [{"Body": "message1"}, {"Body": "message2"}]}


@pytest.fixture
def response_nested_dict():
    return [{"Body": '{"key": "value1"}'}, {"Body": '{"key": "value2"}'}]


@pytest.fixture
def response_dict():
    return [{"Body": "message1"}, {"Body": "message2"}, {"Body": "message3"}]


def test_process_response_with_empty_response():
    response_json = {}
    processed_response = process_response(response_json)
    assert processed_response == []


def test_process_response_with_no_messages(response_json):
    response_json["Messages"] = []
    processed_response = process_response(response_json)
    assert processed_response == []


def test_process_response_with_messages_and_no_filtering(response_json):
    processed_response = process_response(response_json)
    assert processed_response == response_json["Messages"]


def test_process_response_with_messages_and_literal_filtering(response_json):
    processed_response = process_response(
        response_json, message_filtering="literal", message_filtering_match_values=["message1"]
    )
    assert processed_response == [{"Body": "message1"}]


def test_filter_messages_literal():
    messages = [{"Body": "message1"}, {"Body": "message2"}]
    filtered_messages = filter_messages(
        messages,
        message_filtering="literal",
        message_filtering_match_values=["message1"],
        message_filtering_config="",
    )
    assert filtered_messages == [{"Body": "message1"}]


def test_filter_messages_jsonpath(response_nested_dict):
    filtered_messages = filter_messages(
        response_nested_dict,
        message_filtering="jsonpath",
        message_filtering_match_values=["value1"],
        message_filtering_config="key",
    )
    assert filtered_messages == [{"Body": '{"key": "value1"}'}]


def test_filter_messages_literal_with_matching_values(response_dict):
    filtered_messages = filter_messages_literal(
        response_dict, message_filtering_match_values=["message1", "message3"]
    )
    assert filtered_messages == [{"Body": "message1"}, {"Body": "message3"}]


def test_filter_messages_literal_with_no_matching_values(response_dict):
    filtered_messages = filter_messages_literal(response_dict, message_filtering_match_values=["message4"])
    assert filtered_messages == []


def test_filter_messages_literal_with_empty_messages():
    messages = []
    filtered_messages = filter_messages_literal(messages, message_filtering_match_values=["message1"])
    assert filtered_messages == []


def test_filter_messages_jsonpath_with_matching_values(response_nested_dict):
    filtered_messages = filter_messages_jsonpath(
        response_nested_dict,
        message_filtering_match_values=["value1"],
        message_filtering_config="key",
        parse=jsonpath_ng.parse,
    )
    assert filtered_messages == [{"Body": '{"key": "value1"}'}]


def test_filter_messages_jsonpath_with_no_matching_values(response_nested_dict):
    filtered_messages = filter_messages_jsonpath(
        response_nested_dict,
        message_filtering_match_values=["value3"],
        message_filtering_config="key",
        parse=jsonpath_ng.parse,
    )
    assert filtered_messages == []


def test_filter_messages_jsonpath_with_empty_messages():
    messages = []
    filtered_messages = filter_messages_jsonpath(
        messages,
        message_filtering_match_values=["value1"],
        message_filtering_config="key",
        parse=jsonpath_ng.parse,
    )
    assert filtered_messages == []
