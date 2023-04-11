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
    package="airflow.providers.google_vendor.googleads.v12.errors",
    marshal="google.ads.googleads.v12",
    manifest={"UrlFieldErrorEnum",},
)


class UrlFieldErrorEnum(proto.Message):
    r"""Container for enum describing possible url field errors.
    """

    class UrlFieldError(proto.Enum):
        r"""Enum describing possible url field errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_TRACKING_URL_TEMPLATE = 2
        INVALID_TAG_IN_TRACKING_URL_TEMPLATE = 3
        MISSING_TRACKING_URL_TEMPLATE_TAG = 4
        MISSING_PROTOCOL_IN_TRACKING_URL_TEMPLATE = 5
        INVALID_PROTOCOL_IN_TRACKING_URL_TEMPLATE = 6
        MALFORMED_TRACKING_URL_TEMPLATE = 7
        MISSING_HOST_IN_TRACKING_URL_TEMPLATE = 8
        INVALID_TLD_IN_TRACKING_URL_TEMPLATE = 9
        REDUNDANT_NESTED_TRACKING_URL_TEMPLATE_TAG = 10
        INVALID_FINAL_URL = 11
        INVALID_TAG_IN_FINAL_URL = 12
        REDUNDANT_NESTED_FINAL_URL_TAG = 13
        MISSING_PROTOCOL_IN_FINAL_URL = 14
        INVALID_PROTOCOL_IN_FINAL_URL = 15
        MALFORMED_FINAL_URL = 16
        MISSING_HOST_IN_FINAL_URL = 17
        INVALID_TLD_IN_FINAL_URL = 18
        INVALID_FINAL_MOBILE_URL = 19
        INVALID_TAG_IN_FINAL_MOBILE_URL = 20
        REDUNDANT_NESTED_FINAL_MOBILE_URL_TAG = 21
        MISSING_PROTOCOL_IN_FINAL_MOBILE_URL = 22
        INVALID_PROTOCOL_IN_FINAL_MOBILE_URL = 23
        MALFORMED_FINAL_MOBILE_URL = 24
        MISSING_HOST_IN_FINAL_MOBILE_URL = 25
        INVALID_TLD_IN_FINAL_MOBILE_URL = 26
        INVALID_FINAL_APP_URL = 27
        INVALID_TAG_IN_FINAL_APP_URL = 28
        REDUNDANT_NESTED_FINAL_APP_URL_TAG = 29
        MULTIPLE_APP_URLS_FOR_OSTYPE = 30
        INVALID_OSTYPE = 31
        INVALID_PROTOCOL_FOR_APP_URL = 32
        INVALID_PACKAGE_ID_FOR_APP_URL = 33
        URL_CUSTOM_PARAMETERS_COUNT_EXCEEDS_LIMIT = 34
        INVALID_CHARACTERS_IN_URL_CUSTOM_PARAMETER_KEY = 39
        INVALID_CHARACTERS_IN_URL_CUSTOM_PARAMETER_VALUE = 40
        INVALID_TAG_IN_URL_CUSTOM_PARAMETER_VALUE = 41
        REDUNDANT_NESTED_URL_CUSTOM_PARAMETER_TAG = 42
        MISSING_PROTOCOL = 43
        INVALID_PROTOCOL = 52
        INVALID_URL = 44
        DESTINATION_URL_DEPRECATED = 45
        INVALID_TAG_IN_URL = 46
        MISSING_URL_TAG = 47
        DUPLICATE_URL_ID = 48
        INVALID_URL_ID = 49
        FINAL_URL_SUFFIX_MALFORMED = 50
        INVALID_TAG_IN_FINAL_URL_SUFFIX = 51
        INVALID_TOP_LEVEL_DOMAIN = 53
        MALFORMED_TOP_LEVEL_DOMAIN = 54
        MALFORMED_URL = 55
        MISSING_HOST = 56
        NULL_CUSTOM_PARAMETER_VALUE = 57
        VALUE_TRACK_PARAMETER_NOT_SUPPORTED = 58


__all__ = tuple(sorted(__protobuf__.manifest))
