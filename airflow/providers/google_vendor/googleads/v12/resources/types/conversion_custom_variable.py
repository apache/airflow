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

from airflow.providers.google_vendor.googleads.v12.enums.types import (
    conversion_custom_variable_status,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"ConversionCustomVariable",},
)


class ConversionCustomVariable(proto.Message):
    r"""A conversion custom variable
    See "About custom variables for conversions" at
    https://support.google.com/google-ads/answer/9964350

    Attributes:
        resource_name (str):
            Immutable. The resource name of the conversion custom
            variable. Conversion custom variable resource names have the
            form:

            ``customers/{customer_id}/conversionCustomVariables/{conversion_custom_variable_id}``
        id (int):
            Output only. The ID of the conversion custom
            variable.
        name (str):
            Required. The name of the conversion custom
            variable. Name should be unique. The maximum
            length of name is 100 characters. There should
            not be any extra spaces before and after.
        tag (str):
            Required. Immutable. The tag of the
            conversion custom variable. It is used in the
            event snippet and sent to Google Ads along with
            conversion pings. For conversion uploads in
            Google Ads API, the resource name of the
            conversion custom variable is used.
            Tag should be unique. The maximum size of tag is
            100 bytes. There should not be any extra spaces
            before and after. Currently only lowercase
            letters, numbers and underscores are allowed in
            the tag.
        status (google.ads.googleads.v12.enums.types.ConversionCustomVariableStatusEnum.ConversionCustomVariableStatus):
            The status of the conversion custom variable
            for conversion event accrual.
        owner_customer (str):
            Output only. The resource name of the
            customer that owns the conversion custom
            variable.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=2,)
    name = proto.Field(proto.STRING, number=3,)
    tag = proto.Field(proto.STRING, number=4,)
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=conversion_custom_variable_status.ConversionCustomVariableStatusEnum.ConversionCustomVariableStatus,
    )
    owner_customer = proto.Field(proto.STRING, number=6,)


__all__ = tuple(sorted(__protobuf__.manifest))
