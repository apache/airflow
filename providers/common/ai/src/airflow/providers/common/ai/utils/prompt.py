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

from collections.abc import Sequence
from typing import Any

from pydantic_ai.messages import (
    AudioUrl,
    BinaryContent,
    DocumentUrl,
    FileUrl,
    ImageUrl,
    TextContent,
    VideoUrl,
)


def describe_prompt(prompt: str | Sequence[Any]) -> str:
    if isinstance(prompt, str):
        return prompt
    return "\n".join(_describe_part(part) for part in prompt)


def _describe_part(part: Any) -> str:
    if isinstance(part, FileUrl) and part.url[:5].lower() == "data:":
        try:
            part = BinaryContent.from_data_uri(part.url)
        except ValueError:
            return "[data URL]"
    if isinstance(part, str):
        return part
    if isinstance(part, TextContent):
        return part.content
    if isinstance(part, BinaryContent):
        return f"[{part.media_type}, {len(part.data)} bytes]"
    if isinstance(part, (ImageUrl, AudioUrl, DocumentUrl, VideoUrl)):
        return f"[{part.kind}: {part.url}]"
    return f"[{type(part).__name__}]"
