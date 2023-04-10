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
    manifest={"ConversionActionCategoryEnum",},
)


class ConversionActionCategoryEnum(proto.Message):
    r"""Container for enum describing the category of conversions
    that are associated with a ConversionAction.

    """

    class ConversionActionCategory(proto.Enum):
        r"""The category of conversions that are associated with a
        ConversionAction.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        DEFAULT = 2
        PAGE_VIEW = 3
        PURCHASE = 4
        SIGNUP = 5
        DOWNLOAD = 7
        ADD_TO_CART = 8
        BEGIN_CHECKOUT = 9
        SUBSCRIBE_PAID = 10
        PHONE_CALL_LEAD = 11
        IMPORTED_LEAD = 12
        SUBMIT_LEAD_FORM = 13
        BOOK_APPOINTMENT = 14
        REQUEST_QUOTE = 15
        GET_DIRECTIONS = 16
        OUTBOUND_CLICK = 17
        CONTACT = 18
        ENGAGEMENT = 19
        STORE_VISIT = 20
        STORE_SALE = 21
        QUALIFIED_LEAD = 22
        CONVERTED_LEAD = 23


__all__ = tuple(sorted(__protobuf__.manifest))
