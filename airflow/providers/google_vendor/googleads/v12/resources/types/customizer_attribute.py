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

from airflow.providers.google_vendor.googleads.v12.enums.types import customizer_attribute_status
from airflow.providers.google_vendor.googleads.v12.enums.types import customizer_attribute_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CustomizerAttribute",},
)


class CustomizerAttribute(proto.Message):
    r"""A customizer attribute.
    Use CustomerCustomizer, CampaignCustomizer, AdGroupCustomizer,
    or AdGroupCriterionCustomizer to associate a customizer
    attribute and set its value at the customer, campaign, ad group,
    or ad group criterion level, respectively.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the customizer attribute.
            Customizer Attribute resource names have the form:

            ``customers/{customer_id}/customizerAttributes/{customizer_attribute_id}``
        id (int):
            Output only. The ID of the customizer
            attribute.
        name (str):
            Required. Immutable. Name of the customizer
            attribute. Required. It must have a minimum
            length of 1 and maximum length of 40. Name of an
            enabled customizer attribute must be unique
            (case insensitive).
        type_ (google.ads.googleads.v12.enums.types.CustomizerAttributeTypeEnum.CustomizerAttributeType):
            Immutable. The type of the customizer
            attribute.
        status (google.ads.googleads.v12.enums.types.CustomizerAttributeStatusEnum.CustomizerAttributeStatus):
            Output only. The status of the customizer
            attribute.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=2,)
    name = proto.Field(proto.STRING, number=3,)
    type_ = proto.Field(
        proto.ENUM,
        number=4,
        enum=customizer_attribute_type.CustomizerAttributeTypeEnum.CustomizerAttributeType,
    )
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=customizer_attribute_status.CustomizerAttributeStatusEnum.CustomizerAttributeStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
