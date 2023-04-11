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

from airflow.providers.google_vendor.googleads.v12.enums.types import conversion_action_category
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    conversion_value_rule_set_status,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import value_rule_set_attachment_type
from airflow.providers.google_vendor.googleads.v12.enums.types import value_rule_set_dimension


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"ConversionValueRuleSet",},
)


class ConversionValueRuleSet(proto.Message):
    r"""A conversion value rule set

    Attributes:
        resource_name (str):
            Immutable. The resource name of the conversion value rule
            set. Conversion value rule set resource names have the form:

            ``customers/{customer_id}/conversionValueRuleSets/{conversion_value_rule_set_id}``
        id (int):
            Output only. The ID of the conversion value
            rule set.
        conversion_value_rules (Sequence[str]):
            Resource names of rules within the rule set.
        dimensions (Sequence[google.ads.googleads.v12.enums.types.ValueRuleSetDimensionEnum.ValueRuleSetDimension]):
            Defines dimensions for Value Rule conditions.
            The condition types of value rules within this
            value rule set must be of these dimensions. The
            first entry in this list is the primary
            dimension of the included value rules. When
            using value rule primary dimension segmentation,
            conversion values will be segmented into the
            values adjusted by value rules and the original
            values, if some value rules apply.
        owner_customer (str):
            Output only. The resource name of the conversion value rule
            set's owner customer. When the value rule set is inherited
            from a manager customer, owner_customer will be the resource
            name of the manager whereas the customer in the
            resource_name will be of the requesting serving customer.
            \*\* Read-only \*\*
        attachment_type (google.ads.googleads.v12.enums.types.ValueRuleSetAttachmentTypeEnum.ValueRuleSetAttachmentType):
            Immutable. Defines the scope where the
            conversion value rule set is attached.
        campaign (str):
            The resource name of the campaign when the
            conversion value rule set is attached to a
            campaign.
        status (google.ads.googleads.v12.enums.types.ConversionValueRuleSetStatusEnum.ConversionValueRuleSetStatus):
            Output only. The status of the conversion value rule set.
            \*\* Read-only \*\*
        conversion_action_categories (Sequence[google.ads.googleads.v12.enums.types.ConversionActionCategoryEnum.ConversionActionCategory]):
            Immutable. The conversion action categories
            of the conversion value rule set.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=2,)
    conversion_value_rules = proto.RepeatedField(proto.STRING, number=3,)
    dimensions = proto.RepeatedField(
        proto.ENUM,
        number=4,
        enum=value_rule_set_dimension.ValueRuleSetDimensionEnum.ValueRuleSetDimension,
    )
    owner_customer = proto.Field(proto.STRING, number=5,)
    attachment_type = proto.Field(
        proto.ENUM,
        number=6,
        enum=value_rule_set_attachment_type.ValueRuleSetAttachmentTypeEnum.ValueRuleSetAttachmentType,
    )
    campaign = proto.Field(proto.STRING, number=7,)
    status = proto.Field(
        proto.ENUM,
        number=8,
        enum=conversion_value_rule_set_status.ConversionValueRuleSetStatusEnum.ConversionValueRuleSetStatus,
    )
    conversion_action_categories = proto.RepeatedField(
        proto.ENUM,
        number=9,
        enum=conversion_action_category.ConversionActionCategoryEnum.ConversionActionCategory,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
