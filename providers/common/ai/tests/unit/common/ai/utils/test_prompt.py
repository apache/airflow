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
from pydantic_ai.messages import (
    AudioUrl,
    BinaryContent,
    CachePoint,
    DocumentUrl,
    ImageUrl,
    TextContent,
    UploadedFile,
    VideoUrl,
)

from airflow.providers.common.ai.utils.prompt import describe_prompt


@pytest.mark.parametrize(
    ("prompt", "expected"),
    [
        pytest.param("plain text", "plain text", id="str"),
        pytest.param(["first", "second"], "first\nsecond", id="str-parts"),
        pytest.param([TextContent(content="typed text")], "typed text", id="text-content"),
        pytest.param(
            [BinaryContent(data=b"\x89PNG", media_type="image/png")], "[image/png, 4 bytes]", id="binary"
        ),
        pytest.param(
            [ImageUrl(url="https://example.com/a.png")],
            "[image-url: https://example.com/a.png]",
            id="image-url",
        ),
        pytest.param(
            [AudioUrl(url="https://example.com/a.mp3")],
            "[audio-url: https://example.com/a.mp3]",
            id="audio-url",
        ),
        pytest.param(
            [DocumentUrl(url="https://example.com/a.pdf")],
            "[document-url: https://example.com/a.pdf]",
            id="document-url",
        ),
        pytest.param(
            [VideoUrl(url="https://example.com/a.mp4")],
            "[video-url: https://example.com/a.mp4]",
            id="video-url",
        ),
        pytest.param(
            [VideoUrl(url="https://www.youtube.com/watch?v=abc123&t=42")],
            "[video-url: https://www.youtube.com/watch?v=abc123]",
            id="youtube-url",
        ),
        pytest.param(
            [ImageUrl(url="https://user:pass@bucket.s3.amazonaws.com/a.png?X-Amz-Signature=abc#frag")],
            "[image-url: https://bucket.s3.amazonaws.com/a.png]",
            id="signed-url",
        ),
        pytest.param(
            [ImageUrl(url="data:image/png;base64,iVBORw0KGgo=")], "[image/png, 8 bytes]", id="data-url"
        ),
        pytest.param([DocumentUrl(url="data:text/plain,hello")], "[data URL]", id="data-url-not-base64"),
        pytest.param(
            [ImageUrl(url="data:image/png;base64,not-base64!")], "[data URL]", id="data-url-invalid-payload"
        ),
        pytest.param(
            [ImageUrl(url="DATA:image/png;base64,iVBORw0KGgo=")], "[data URL]", id="data-url-uppercase-scheme"
        ),
        pytest.param(
            [UploadedFile(file_id="file-abc", provider_name="openai")],
            "[uploaded-file: file-abc]",
            id="uploaded-file",
        ),
        pytest.param([CachePoint()], "[CachePoint]", id="cache-point"),
    ],
)
def test_describe_prompt(prompt, expected):
    assert describe_prompt(prompt) == expected
