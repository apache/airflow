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
"""
Discord embed structure.

See:
    https://discord.com/developers/docs/resources/message#embed-object-embed-structure
"""

from __future__ import annotations

from typing import Literal, TypedDict

from typing_extensions import NotRequired, Required

EmbedType = Literal["rich"]


class EmbedFooter(TypedDict):
    """
    EmbedFooter.

    :param text: Footer text.
    :param icon_url: Url of footer icon (only supports http(s) and attachments).
    :param proxy_icon_url: A proxy url of footer icon.

    See:
        https://discord.com/developers/docs/resources/message#embed-object-embed-footer-structure
    """

    text: str
    icon_url: NotRequired[str]
    proxy_icon_url: NotRequired[str]


class EmbedField(TypedDict):
    """
    EmbedField.

    :param name: Name of the field.
    :param value: Value of the field.
    :param inline: Whether or not this field should display inline.

    See:
        https://discord.com/developers/docs/resources/message#embed-object-embed-field-structure
    """

    name: str
    value: str
    inline: NotRequired[bool]


class EmbedProvider(TypedDict, total=False):
    """
    EmbedProvider.

    :param name: Name of provider
    :param url: Url of provider

    See:
        https://discord.com/developers/docs/resources/message#embed-object-embed-provider-structure
    """

    name: str
    url: str


class EmbedAuthor(TypedDict, total=False):
    """
    EmbedAuthor.

    :param name: Name of author.
    :param url: Url of author (only supports http(s)).
    :param icon_url: Url of author icon (only supports http(s) and attachments).
    :param proxy_icon_url: A proxy url of author icon.

    See:
        https://discord.com/developers/docs/resources/message#embed-object-embed-author-structure
    """

    name: Required[str]
    url: str
    icon_url: str
    proxy_icon_url: str


class Embed(TypedDict, total=False):
    """
    Embed.

    :param title: The text that is placed above the description.
       Embed titles are limited to 256 characters.
    :param description: The part of the embed where most of the text is contained.
       Embed descriptions are limited to 2048 characters.
    :param type: Type of embed (always "rich" for webhook embeds).
    :param url: Url of embed.
    :param timestamp: Timestamp (ISO8601) of embed content.
    :param color: Color code of the embed.
    :param footer: Footer information. Footer text is limited to 2048 characters
    :param provider: Add provider information.
    :param author: Adds the author block to the embed, always located at the
       top of the embed.
    :param fields: Add fields information, max of 25 fields.

    See:
        https://discord.com/developers/docs/resources/message#embed-object-embed-author-structure

    """

    title: str
    description: str
    type: EmbedType
    url: str
    timestamp: str
    color: int
    footer: EmbedFooter
    provider: EmbedProvider
    author: EmbedAuthor
    fields: list[EmbedField]
