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

from airflow.providers.google_vendor.googleads.v12.common.types import customizer_value
from airflow.providers.google_vendor.googleads.v12.enums.types import customizer_value_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AdGroupCustomizer",},
)


class AdGroupCustomizer(proto.Message):
    r"""A customizer value for the associated CustomizerAttribute at
    the AdGroup level.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the ad group customizer. Ad
            group customizer resource names have the form:

            ``customers/{customer_id}/adGroupCustomizers/{ad_group_id}~{customizer_attribute_id}``
        ad_group (str):
            Immutable. The ad group to which the
            customizer attribute is linked.
        customizer_attribute (str):
            Required. Immutable. The customizer attribute
            which is linked to the ad group.
        status (google.ads.googleads.v12.enums.types.CustomizerValueStatusEnum.CustomizerValueStatus):
            Output only. The status of the ad group
            customizer.
        value (google.ads.googleads.v12.common.types.CustomizerValue):
            Required. The value to associate with the
            customizer attribute at this level. The value
            must be of the type specified for the
            CustomizerAttribute.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    ad_group = proto.Field(proto.STRING, number=2,)
    customizer_attribute = proto.Field(proto.STRING, number=3,)
    status = proto.Field(
        proto.ENUM,
        number=4,
        enum=customizer_value_status.CustomizerValueStatusEnum.CustomizerValueStatus,
    )
    value = proto.Field(
        proto.MESSAGE, number=5, message=customizer_value.CustomizerValue,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
