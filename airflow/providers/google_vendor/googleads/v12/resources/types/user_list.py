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

from airflow.providers.google_vendor.googleads.v12.common.types import user_lists
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    access_reason as gage_access_reason,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import user_list_access_status
from airflow.providers.google_vendor.googleads.v12.enums.types import user_list_closing_reason
from airflow.providers.google_vendor.googleads.v12.enums.types import user_list_membership_status
from airflow.providers.google_vendor.googleads.v12.enums.types import user_list_size_range
from airflow.providers.google_vendor.googleads.v12.enums.types import user_list_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"UserList",},
)


class UserList(proto.Message):
    r"""A user list. This is a list of users a customer may target.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the user list. User list
            resource names have the form:

            ``customers/{customer_id}/userLists/{user_list_id}``
        id (int):
            Output only. Id of the user list.

            This field is a member of `oneof`_ ``_id``.
        read_only (bool):
            Output only. An option that indicates if a
            user may edit a list. Depends on the list
            ownership and list type. For example, external
            remarketing user lists are not editable.

            This field is read-only.

            This field is a member of `oneof`_ ``_read_only``.
        name (str):
            Name of this user list. Depending on its access_reason, the
            user list name may not be unique (for example, if
            access_reason=SHARED)

            This field is a member of `oneof`_ ``_name``.
        description (str):
            Description of this user list.

            This field is a member of `oneof`_ ``_description``.
        membership_status (google.ads.googleads.v12.enums.types.UserListMembershipStatusEnum.UserListMembershipStatus):
            Membership status of this user list.
            Indicates whether a user list is open or active.
            Only open user lists can accumulate more users
            and can be targeted to.
        integration_code (str):
            An ID from external system. It is used by
            user list sellers to correlate IDs on their
            systems.

            This field is a member of `oneof`_ ``_integration_code``.
        membership_life_span (int):
            Number of days a user's cookie stays on your list since its
            most recent addition to the list. This field must be between
            0 and 540 inclusive. However, for CRM based userlists, this
            field can be set to 10000 which means no expiration.

            It'll be ignored for logical_user_list.

            This field is a member of `oneof`_ ``_membership_life_span``.
        size_for_display (int):
            Output only. Estimated number of users in
            this user list, on the Google Display Network.
            This value is null if the number of users has
            not yet been determined.
            This field is read-only.

            This field is a member of `oneof`_ ``_size_for_display``.
        size_range_for_display (google.ads.googleads.v12.enums.types.UserListSizeRangeEnum.UserListSizeRange):
            Output only. Size range in terms of number of
            users of the UserList, on the Google Display
            Network.
            This field is read-only.
        size_for_search (int):
            Output only. Estimated number of users in
            this user list in the google.com domain. These
            are the users available for targeting in Search
            campaigns. This value is null if the number of
            users has not yet been determined.
            This field is read-only.

            This field is a member of `oneof`_ ``_size_for_search``.
        size_range_for_search (google.ads.googleads.v12.enums.types.UserListSizeRangeEnum.UserListSizeRange):
            Output only. Size range in terms of number of
            users of the UserList, for Search ads.
            This field is read-only.
        type_ (google.ads.googleads.v12.enums.types.UserListTypeEnum.UserListType):
            Output only. Type of this list.
            This field is read-only.
        closing_reason (google.ads.googleads.v12.enums.types.UserListClosingReasonEnum.UserListClosingReason):
            Indicating the reason why this user list
            membership status is closed. It is only
            populated on lists that were automatically
            closed due to inactivity, and will be cleared
            once the list membership status becomes open.
        access_reason (google.ads.googleads.v12.enums.types.AccessReasonEnum.AccessReason):
            Output only. Indicates the reason this
            account has been granted access to the list. The
            reason can be SHARED, OWNED, LICENSED or
            SUBSCRIBED.
            This field is read-only.
        account_user_list_status (google.ads.googleads.v12.enums.types.UserListAccessStatusEnum.UserListAccessStatus):
            Indicates if this share is still enabled.
            When a UserList is shared with the user this
            field is set to ENABLED. Later the userList
            owner can decide to revoke the share and make it
            DISABLED.
            The default value of this field is set to
            ENABLED.
        eligible_for_search (bool):
            Indicates if this user list is eligible for
            Google Search Network.

            This field is a member of `oneof`_ ``_eligible_for_search``.
        eligible_for_display (bool):
            Output only. Indicates this user list is
            eligible for Google Display Network.
            This field is read-only.

            This field is a member of `oneof`_ ``_eligible_for_display``.
        match_rate_percentage (int):
            Output only. Indicates match rate for Customer Match lists.
            The range of this field is [0-100]. This will be null for
            other list types or when it's not possible to calculate the
            match rate.

            This field is read-only.

            This field is a member of `oneof`_ ``_match_rate_percentage``.
        crm_based_user_list (google.ads.googleads.v12.common.types.CrmBasedUserListInfo):
            User list of CRM users provided by the
            advertiser.

            This field is a member of `oneof`_ ``user_list``.
        similar_user_list (google.ads.googleads.v12.common.types.SimilarUserListInfo):
            Output only. User list which are similar to
            users from another UserList. These lists are
            readonly and automatically created by google.

            This field is a member of `oneof`_ ``user_list``.
        rule_based_user_list (google.ads.googleads.v12.common.types.RuleBasedUserListInfo):
            User list generated by a rule.

            This field is a member of `oneof`_ ``user_list``.
        logical_user_list (google.ads.googleads.v12.common.types.LogicalUserListInfo):
            User list that is a custom combination of
            user lists and user interests.

            This field is a member of `oneof`_ ``user_list``.
        basic_user_list (google.ads.googleads.v12.common.types.BasicUserListInfo):
            User list targeting as a collection of
            conversion or remarketing actions.

            This field is a member of `oneof`_ ``user_list``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=25, optional=True,)
    read_only = proto.Field(proto.BOOL, number=26, optional=True,)
    name = proto.Field(proto.STRING, number=27, optional=True,)
    description = proto.Field(proto.STRING, number=28, optional=True,)
    membership_status = proto.Field(
        proto.ENUM,
        number=6,
        enum=user_list_membership_status.UserListMembershipStatusEnum.UserListMembershipStatus,
    )
    integration_code = proto.Field(proto.STRING, number=29, optional=True,)
    membership_life_span = proto.Field(proto.INT64, number=30, optional=True,)
    size_for_display = proto.Field(proto.INT64, number=31, optional=True,)
    size_range_for_display = proto.Field(
        proto.ENUM,
        number=10,
        enum=user_list_size_range.UserListSizeRangeEnum.UserListSizeRange,
    )
    size_for_search = proto.Field(proto.INT64, number=32, optional=True,)
    size_range_for_search = proto.Field(
        proto.ENUM,
        number=12,
        enum=user_list_size_range.UserListSizeRangeEnum.UserListSizeRange,
    )
    type_ = proto.Field(
        proto.ENUM,
        number=13,
        enum=user_list_type.UserListTypeEnum.UserListType,
    )
    closing_reason = proto.Field(
        proto.ENUM,
        number=14,
        enum=user_list_closing_reason.UserListClosingReasonEnum.UserListClosingReason,
    )
    access_reason = proto.Field(
        proto.ENUM,
        number=15,
        enum=gage_access_reason.AccessReasonEnum.AccessReason,
    )
    account_user_list_status = proto.Field(
        proto.ENUM,
        number=16,
        enum=user_list_access_status.UserListAccessStatusEnum.UserListAccessStatus,
    )
    eligible_for_search = proto.Field(proto.BOOL, number=33, optional=True,)
    eligible_for_display = proto.Field(proto.BOOL, number=34, optional=True,)
    match_rate_percentage = proto.Field(proto.INT32, number=24, optional=True,)
    crm_based_user_list = proto.Field(
        proto.MESSAGE,
        number=19,
        oneof="user_list",
        message=user_lists.CrmBasedUserListInfo,
    )
    similar_user_list = proto.Field(
        proto.MESSAGE,
        number=20,
        oneof="user_list",
        message=user_lists.SimilarUserListInfo,
    )
    rule_based_user_list = proto.Field(
        proto.MESSAGE,
        number=21,
        oneof="user_list",
        message=user_lists.RuleBasedUserListInfo,
    )
    logical_user_list = proto.Field(
        proto.MESSAGE,
        number=22,
        oneof="user_list",
        message=user_lists.LogicalUserListInfo,
    )
    basic_user_list = proto.Field(
        proto.MESSAGE,
        number=23,
        oneof="user_list",
        message=user_lists.BasicUserListInfo,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
