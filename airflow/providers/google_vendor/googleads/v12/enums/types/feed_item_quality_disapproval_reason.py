# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import proto  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.enums",
    marshal="google.ads.googleads.v12",
    manifest={"FeedItemQualityDisapprovalReasonEnum",},
)


class FeedItemQualityDisapprovalReasonEnum(proto.Message):
    r"""Container for enum describing possible quality evaluation
    disapproval reasons of a feed item.

    """

    class FeedItemQualityDisapprovalReason(proto.Enum):
        r"""The possible quality evaluation disapproval reasons of a feed
        item.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        PRICE_TABLE_REPETITIVE_HEADERS = 2
        PRICE_TABLE_REPETITIVE_DESCRIPTION = 3
        PRICE_TABLE_INCONSISTENT_ROWS = 4
        PRICE_DESCRIPTION_HAS_PRICE_QUALIFIERS = 5
        PRICE_UNSUPPORTED_LANGUAGE = 6
        PRICE_TABLE_ROW_HEADER_TABLE_TYPE_MISMATCH = 7
        PRICE_TABLE_ROW_HEADER_HAS_PROMOTIONAL_TEXT = 8
        PRICE_TABLE_ROW_DESCRIPTION_NOT_RELEVANT = 9
        PRICE_TABLE_ROW_DESCRIPTION_HAS_PROMOTIONAL_TEXT = 10
        PRICE_TABLE_ROW_HEADER_DESCRIPTION_REPETITIVE = 11
        PRICE_TABLE_ROW_UNRATEABLE = 12
        PRICE_TABLE_ROW_PRICE_INVALID = 13
        PRICE_TABLE_ROW_URL_INVALID = 14
        PRICE_HEADER_OR_DESCRIPTION_HAS_PRICE = 15
        STRUCTURED_SNIPPETS_HEADER_POLICY_VIOLATED = 16
        STRUCTURED_SNIPPETS_REPEATED_VALUES = 17
        STRUCTURED_SNIPPETS_EDITORIAL_GUIDELINES = 18
        STRUCTURED_SNIPPETS_HAS_PROMOTIONAL_TEXT = 19


__all__ = tuple(sorted(__protobuf__.manifest))
