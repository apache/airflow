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

import json
import logging
from typing import Any

import jsonpath_ng
import jsonpath_ng.ext
from typing_extensions import Literal

log = logging.getLogger(__name__)


MessageFilteringType = Literal["literal", "jsonpath", "jsonpath-ext"]


def process_response(
    response: Any,
    message_filtering: MessageFilteringType | None = None,
    message_filtering_match_values: Any = None,
    message_filtering_config: Any = None,
) -> Any:
    """
    Process the response from SQS.

    :param response: The response from SQS
    :return: The processed response
    """
    if not isinstance(response, dict) or "Messages" not in response:
        return []

    messages = response["Messages"]
    num_messages = len(messages)
    log.info("Received %d messages", num_messages)

    if num_messages and message_filtering:
        messages = filter_messages(
            messages,
            message_filtering,
            message_filtering_match_values,
            message_filtering_config,
        )
        num_messages = len(messages)
        log.info("There are %d messages left after filtering", num_messages)
    return messages


def filter_messages(
    messages, message_filtering, message_filtering_match_values, message_filtering_config
) -> list[Any]:
    if message_filtering == "literal":
        return filter_messages_literal(messages, message_filtering_match_values)
    if message_filtering == "jsonpath":
        return filter_messages_jsonpath(
            messages,
            message_filtering_match_values,
            message_filtering_config,
            jsonpath_ng.parse,
        )
    if message_filtering == "jsonpath-ext":
        return filter_messages_jsonpath(
            messages,
            message_filtering_match_values,
            message_filtering_config,
            jsonpath_ng.ext.parse,
        )
    else:
        raise NotImplementedError("Override this method to define custom filters")


def filter_messages_literal(messages, message_filtering_match_values) -> list[Any]:
    return [
        message
        for message in messages
        if message["Body"] in message_filtering_match_values
    ]


def filter_messages_jsonpath(
    messages, message_filtering_match_values, message_filtering_config, parse
) -> list[Any]:
    jsonpath_expr = parse(message_filtering_config)
    filtered_messages = []
    for message in messages:
        body = message["Body"]
        # Body is a string, deserialize to an object and then parse
        body = json.loads(body)
        results = jsonpath_expr.find(body)
        if results and (
            message_filtering_match_values is None
            or any(result.value in message_filtering_match_values for result in results)
        ):
            filtered_messages.append(message)
    return filtered_messages
