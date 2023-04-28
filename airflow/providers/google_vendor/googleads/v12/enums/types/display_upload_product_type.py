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
    manifest={"DisplayUploadProductTypeEnum",},
)


class DisplayUploadProductTypeEnum(proto.Message):
    r"""Container for display upload product types. Product types
    that have the word "DYNAMIC" in them must be associated with a
    campaign that has a dynamic remarketing feed. See
    https://support.google.com/google-ads/answer/6053288 for more
    info about dynamic remarketing. Other product types are regarded
    as "static" and do not have this requirement.

    """

    class DisplayUploadProductType(proto.Enum):
        r"""Enumerates display upload product types."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        HTML5_UPLOAD_AD = 2
        DYNAMIC_HTML5_EDUCATION_AD = 3
        DYNAMIC_HTML5_FLIGHT_AD = 4
        DYNAMIC_HTML5_HOTEL_RENTAL_AD = 5
        DYNAMIC_HTML5_JOB_AD = 6
        DYNAMIC_HTML5_LOCAL_AD = 7
        DYNAMIC_HTML5_REAL_ESTATE_AD = 8
        DYNAMIC_HTML5_CUSTOM_AD = 9
        DYNAMIC_HTML5_TRAVEL_AD = 10
        DYNAMIC_HTML5_HOTEL_AD = 11


__all__ = tuple(sorted(__protobuf__.manifest))
