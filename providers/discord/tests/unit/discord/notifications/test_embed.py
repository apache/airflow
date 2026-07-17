#
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

from airflow.providers.discord.notifications.embed import (
    Embed,
    EmbedAuthor,
    EmbedField,
    EmbedFooter,
    EmbedProvider,
)


def test_embed_footer():
    footer = EmbedFooter(
        text="Test footer",
        icon_url="https://example.com/icon.png",
        proxy_icon_url="https://example.com/icon.png",
    )
    assert footer["text"] == "Test footer"
    assert footer["icon_url"] == "https://example.com/icon.png"
    assert footer["proxy_icon_url"] == "https://example.com/icon.png"


def test_embed_field():
    field = EmbedField(name="Test Field", value="Test Value")
    assert field["name"] == "Test Field"
    assert field["value"] == "Test Value"


def test_embed_provider():
    provider = EmbedProvider(name="Test Provider", url="https://example.com")
    assert provider["name"] == "Test Provider"
    assert provider["url"] == "https://example.com"


def test_embed_author():
    author = EmbedAuthor(
        name="Test Author",
        url="https://example.com",
        icon_url="https://example.com/icon.png",
        proxy_icon_url="https://example.com/icon.png",
    )
    assert author["name"] == "Test Author"
    assert author["url"] == "https://example.com"
    assert author["icon_url"] == "https://example.com/icon.png"
    assert author["proxy_icon_url"] == "https://example.com/icon.png"


def test_embed():
    embed = Embed(title="Test Title", description="Test Description")
    assert embed["title"] == "Test Title"
    assert embed["description"] == "Test Description"
