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

from airflow.providers.google_vendor.googleads.v12.common.types import custom_parameter
from airflow.providers.google_vendor.googleads.v12.common.types import feed_common
from airflow.providers.google_vendor.googleads.v12.common.types import policy
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    feed_item_quality_approval_status,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    feed_item_quality_disapproval_reason,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import feed_item_status
from airflow.providers.google_vendor.googleads.v12.enums.types import feed_item_validation_status
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    geo_targeting_restriction as gage_geo_targeting_restriction,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import placeholder_type
from airflow.providers.google_vendor.googleads.v12.enums.types import policy_approval_status
from airflow.providers.google_vendor.googleads.v12.enums.types import policy_review_status
from airflow.providers.google_vendor.googleads.v12.errors.types import feed_item_validation_error


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={
        "FeedItem",
        "FeedItemAttributeValue",
        "FeedItemPlaceholderPolicyInfo",
        "FeedItemValidationError",
    },
)


class FeedItem(proto.Message):
    r"""A feed item.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the feed item. Feed item
            resource names have the form:

            ``customers/{customer_id}/feedItems/{feed_id}~{feed_item_id}``
        feed (str):
            Immutable. The feed to which this feed item
            belongs.

            This field is a member of `oneof`_ ``_feed``.
        id (int):
            Output only. The ID of this feed item.

            This field is a member of `oneof`_ ``_id``.
        start_date_time (str):
            Start time in which this feed item is
            effective and can begin serving. The time is in
            the customer's time zone. The format is
            "YYYY-MM-DD HH:MM:SS".
            Examples: "2018-03-05 09:15:00" or "2018-02-01
            14:34:30".

            This field is a member of `oneof`_ ``_start_date_time``.
        end_date_time (str):
            End time in which this feed item is no longer
            effective and will stop serving. The time is in
            the customer's time zone. The format is
            "YYYY-MM-DD HH:MM:SS".
            Examples: "2018-03-05 09:15:00" or "2018-02-01
            14:34:30".

            This field is a member of `oneof`_ ``_end_date_time``.
        attribute_values (Sequence[google.ads.googleads.v12.resources.types.FeedItemAttributeValue]):
            The feed item's attribute values.
        geo_targeting_restriction (google.ads.googleads.v12.enums.types.GeoTargetingRestrictionEnum.GeoTargetingRestriction):
            Geo targeting restriction specifies the type
            of location that can be used for targeting.
        url_custom_parameters (Sequence[google.ads.googleads.v12.common.types.CustomParameter]):
            The list of mappings used to substitute custom parameter
            tags in a ``tracking_url_template``, ``final_urls``, or
            ``mobile_final_urls``.
        status (google.ads.googleads.v12.enums.types.FeedItemStatusEnum.FeedItemStatus):
            Output only. Status of the feed item.
            This field is read-only.
        policy_infos (Sequence[google.ads.googleads.v12.resources.types.FeedItemPlaceholderPolicyInfo]):
            Output only. List of info about a feed item's
            validation and approval state for active feed
            mappings. There will be an entry in the list for
            each type of feed mapping associated with the
            feed, for example, a feed with a sitelink and a
            call feed mapping would cause every feed item
            associated with that feed to have an entry in
            this list for both sitelink and call. This field
            is read-only.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    feed = proto.Field(proto.STRING, number=11, optional=True,)
    id = proto.Field(proto.INT64, number=12, optional=True,)
    start_date_time = proto.Field(proto.STRING, number=13, optional=True,)
    end_date_time = proto.Field(proto.STRING, number=14, optional=True,)
    attribute_values = proto.RepeatedField(
        proto.MESSAGE, number=6, message="FeedItemAttributeValue",
    )
    geo_targeting_restriction = proto.Field(
        proto.ENUM,
        number=7,
        enum=gage_geo_targeting_restriction.GeoTargetingRestrictionEnum.GeoTargetingRestriction,
    )
    url_custom_parameters = proto.RepeatedField(
        proto.MESSAGE, number=8, message=custom_parameter.CustomParameter,
    )
    status = proto.Field(
        proto.ENUM,
        number=9,
        enum=feed_item_status.FeedItemStatusEnum.FeedItemStatus,
    )
    policy_infos = proto.RepeatedField(
        proto.MESSAGE, number=10, message="FeedItemPlaceholderPolicyInfo",
    )


class FeedItemAttributeValue(proto.Message):
    r"""A feed item attribute value.

    Attributes:
        feed_attribute_id (int):
            Id of the feed attribute for which the value
            is associated with.

            This field is a member of `oneof`_ ``_feed_attribute_id``.
        integer_value (int):
            Int64 value. Should be set if feed_attribute_id refers to a
            feed attribute of type INT64.

            This field is a member of `oneof`_ ``_integer_value``.
        boolean_value (bool):
            Bool value. Should be set if feed_attribute_id refers to a
            feed attribute of type BOOLEAN.

            This field is a member of `oneof`_ ``_boolean_value``.
        string_value (str):
            String value. Should be set if feed_attribute_id refers to a
            feed attribute of type STRING, URL or DATE_TIME. For STRING
            the maximum length is 1500 characters. For URL the maximum
            length is 2076 characters. For DATE_TIME the string must be
            in the format "YYYYMMDD HHMMSS".

            This field is a member of `oneof`_ ``_string_value``.
        double_value (float):
            Double value. Should be set if feed_attribute_id refers to a
            feed attribute of type DOUBLE.

            This field is a member of `oneof`_ ``_double_value``.
        price_value (google.ads.googleads.v12.common.types.Money):
            Price value. Should be set if feed_attribute_id refers to a
            feed attribute of type PRICE.
        integer_values (Sequence[int]):
            Repeated int64 value. Should be set if feed_attribute_id
            refers to a feed attribute of type INT64_LIST.
        boolean_values (Sequence[bool]):
            Repeated bool value. Should be set if feed_attribute_id
            refers to a feed attribute of type BOOLEAN_LIST.
        string_values (Sequence[str]):
            Repeated string value. Should be set if feed_attribute_id
            refers to a feed attribute of type STRING_LIST, URL_LIST or
            DATE_TIME_LIST. For STRING_LIST and URL_LIST the total size
            of the list in bytes may not exceed 3000. For DATE_TIME_LIST
            the number of elements may not exceed 200.

            For STRING_LIST the maximum length of each string element is
            1500 characters. For URL_LIST the maximum length is 2076
            characters. For DATE_TIME the format of the string must be
            the same as start and end time for the feed item.
        double_values (Sequence[float]):
            Repeated double value. Should be set if feed_attribute_id
            refers to a feed attribute of type DOUBLE_LIST.
    """

    feed_attribute_id = proto.Field(proto.INT64, number=11, optional=True,)
    integer_value = proto.Field(proto.INT64, number=12, optional=True,)
    boolean_value = proto.Field(proto.BOOL, number=13, optional=True,)
    string_value = proto.Field(proto.STRING, number=14, optional=True,)
    double_value = proto.Field(proto.DOUBLE, number=15, optional=True,)
    price_value = proto.Field(
        proto.MESSAGE, number=6, message=feed_common.Money,
    )
    integer_values = proto.RepeatedField(proto.INT64, number=16,)
    boolean_values = proto.RepeatedField(proto.BOOL, number=17,)
    string_values = proto.RepeatedField(proto.STRING, number=18,)
    double_values = proto.RepeatedField(proto.DOUBLE, number=19,)


class FeedItemPlaceholderPolicyInfo(proto.Message):
    r"""Policy, validation, and quality approval info for a feed item
    for the specified placeholder type.

    Attributes:
        placeholder_type_enum (google.ads.googleads.v12.enums.types.PlaceholderTypeEnum.PlaceholderType):
            Output only. The placeholder type.
        feed_mapping_resource_name (str):
            Output only. The FeedMapping that contains
            the placeholder type.

            This field is a member of `oneof`_ ``_feed_mapping_resource_name``.
        review_status (google.ads.googleads.v12.enums.types.PolicyReviewStatusEnum.PolicyReviewStatus):
            Output only. Where the placeholder type is in
            the review process.
        approval_status (google.ads.googleads.v12.enums.types.PolicyApprovalStatusEnum.PolicyApprovalStatus):
            Output only. The overall approval status of
            the placeholder type, calculated based on the
            status of its individual policy topic entries.
        policy_topic_entries (Sequence[google.ads.googleads.v12.common.types.PolicyTopicEntry]):
            Output only. The list of policy findings for
            the placeholder type.
        validation_status (google.ads.googleads.v12.enums.types.FeedItemValidationStatusEnum.FeedItemValidationStatus):
            Output only. The validation status of the
            palceholder type.
        validation_errors (Sequence[google.ads.googleads.v12.resources.types.FeedItemValidationError]):
            Output only. List of placeholder type
            validation errors.
        quality_approval_status (google.ads.googleads.v12.enums.types.FeedItemQualityApprovalStatusEnum.FeedItemQualityApprovalStatus):
            Output only. Placeholder type quality
            evaluation approval status.
        quality_disapproval_reasons (Sequence[google.ads.googleads.v12.enums.types.FeedItemQualityDisapprovalReasonEnum.FeedItemQualityDisapprovalReason]):
            Output only. List of placeholder type quality
            evaluation disapproval reasons.
    """

    placeholder_type_enum = proto.Field(
        proto.ENUM,
        number=10,
        enum=placeholder_type.PlaceholderTypeEnum.PlaceholderType,
    )
    feed_mapping_resource_name = proto.Field(
        proto.STRING, number=11, optional=True,
    )
    review_status = proto.Field(
        proto.ENUM,
        number=3,
        enum=policy_review_status.PolicyReviewStatusEnum.PolicyReviewStatus,
    )
    approval_status = proto.Field(
        proto.ENUM,
        number=4,
        enum=policy_approval_status.PolicyApprovalStatusEnum.PolicyApprovalStatus,
    )
    policy_topic_entries = proto.RepeatedField(
        proto.MESSAGE, number=5, message=policy.PolicyTopicEntry,
    )
    validation_status = proto.Field(
        proto.ENUM,
        number=6,
        enum=feed_item_validation_status.FeedItemValidationStatusEnum.FeedItemValidationStatus,
    )
    validation_errors = proto.RepeatedField(
        proto.MESSAGE, number=7, message="FeedItemValidationError",
    )
    quality_approval_status = proto.Field(
        proto.ENUM,
        number=8,
        enum=feed_item_quality_approval_status.FeedItemQualityApprovalStatusEnum.FeedItemQualityApprovalStatus,
    )
    quality_disapproval_reasons = proto.RepeatedField(
        proto.ENUM,
        number=9,
        enum=feed_item_quality_disapproval_reason.FeedItemQualityDisapprovalReasonEnum.FeedItemQualityDisapprovalReason,
    )


class FeedItemValidationError(proto.Message):
    r"""Stores a validation error and the set of offending feed
    attributes which together are responsible for causing a feed
    item validation error.

    Attributes:
        validation_error (google.ads.googleads.v12.errors.types.FeedItemValidationErrorEnum.FeedItemValidationError):
            Output only. Error code indicating what
            validation error was triggered. The description
            of the error can be found in the 'description'
            field.
        description (str):
            Output only. The description of the
            validation error.

            This field is a member of `oneof`_ ``_description``.
        feed_attribute_ids (Sequence[int]):
            Output only. Set of feed attributes in the
            feed item flagged during validation. If empty,
            no specific feed attributes can be associated
            with the error (for example, error across the
            entire feed item).
        extra_info (str):
            Output only. Any extra information related to this error
            which is not captured by validation_error and
            feed_attribute_id (for example, placeholder field IDs when
            feed_attribute_id is not mapped). Note that extra_info is
            not localized.

            This field is a member of `oneof`_ ``_extra_info``.
    """

    validation_error = proto.Field(
        proto.ENUM,
        number=1,
        enum=feed_item_validation_error.FeedItemValidationErrorEnum.FeedItemValidationError,
    )
    description = proto.Field(proto.STRING, number=6, optional=True,)
    feed_attribute_ids = proto.RepeatedField(proto.INT64, number=7,)
    extra_info = proto.Field(proto.STRING, number=8, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
