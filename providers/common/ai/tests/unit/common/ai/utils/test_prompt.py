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
from pydantic_ai.messages import BinaryContent, CachePoint, DocumentUrl, ImageUrl, TextContent

from airflow.providers.common.ai.utils.prompt import describe_prompt


@pytest.mark.parametrize(
    ("prompt", "expected"),
    [
        ("plain text", "plain text"),
        (["first", "second"], "first\nsecond"),
        ([TextContent(content="typed text")], "typed text"),
        ([BinaryContent(data=b"\x89PNG", media_type="image/png")], "[image/png, 4 bytes]"),
        ([ImageUrl(url="https://example.com/a.png")], "[image-url: https://example.com/a.png]"),
        ([ImageUrl(url="data:image/png;base64,iVBORw0KGgo=")], "[image/png, 8 bytes]"),
        ([DocumentUrl(url="data:text/plain,hello")], "[data URL]"),
        ([ImageUrl(url="DATA:image/png;base64,not-base64!")], "[data URL]"),
        ([CachePoint()], "[CachePoint]"),
    ],
)
def test_describe_prompt(prompt, expected):
    assert describe_prompt(prompt) == expected
