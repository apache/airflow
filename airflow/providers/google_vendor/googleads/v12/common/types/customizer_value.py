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

from airflow.providers.google_vendor.googleads.v12.enums.types import customizer_attribute_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={"CustomizerValue",},
)


class CustomizerValue(proto.Message):
    r"""A customizer value that is referenced in customizer linkage
    entities like CustomerCustomizer, CampaignCustomizer, etc.

    Attributes:
        type_ (google.ads.googleads.v12.enums.types.CustomizerAttributeTypeEnum.CustomizerAttributeType):
            Required. The data type for the customizer value. It must
            match the attribute type. The string_value content must
            match the constraints associated with the type.
        string_value (str):
            Required. Value to insert in creative text.
            Customizer values of all types are stored as
            string to make formatting unambiguous.
    """

    type_ = proto.Field(
        proto.ENUM,
        number=1,
        enum=customizer_attribute_type.CustomizerAttributeTypeEnum.CustomizerAttributeType,
    )
    string_value = proto.Field(proto.STRING, number=2,)


__all__ = tuple(sorted(__protobuf__.manifest))
