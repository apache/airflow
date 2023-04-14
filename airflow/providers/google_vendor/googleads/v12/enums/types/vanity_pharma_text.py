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
    manifest={"VanityPharmaTextEnum",},
)


class VanityPharmaTextEnum(proto.Message):
    r"""The text that will be displayed in display URL of the text ad
    when website description is the selected display mode for vanity
    pharma URLs.

    """

    class VanityPharmaText(proto.Enum):
        r"""Enum describing possible text."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        PRESCRIPTION_TREATMENT_WEBSITE_EN = 2
        PRESCRIPTION_TREATMENT_WEBSITE_ES = 3
        PRESCRIPTION_DEVICE_WEBSITE_EN = 4
        PRESCRIPTION_DEVICE_WEBSITE_ES = 5
        MEDICAL_DEVICE_WEBSITE_EN = 6
        MEDICAL_DEVICE_WEBSITE_ES = 7
        PREVENTATIVE_TREATMENT_WEBSITE_EN = 8
        PREVENTATIVE_TREATMENT_WEBSITE_ES = 9
        PRESCRIPTION_CONTRACEPTION_WEBSITE_EN = 10
        PRESCRIPTION_CONTRACEPTION_WEBSITE_ES = 11
        PRESCRIPTION_VACCINE_WEBSITE_EN = 12
        PRESCRIPTION_VACCINE_WEBSITE_ES = 13


__all__ = tuple(sorted(__protobuf__.manifest))
