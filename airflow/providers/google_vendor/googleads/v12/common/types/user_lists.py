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

from airflow.providers.google_vendor.googleads.v12.enums.types import customer_match_upload_key_type
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    user_list_combined_rule_operator,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import user_list_crm_data_source_type
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    user_list_date_rule_item_operator,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    user_list_flexible_rule_operator,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import user_list_logical_rule_operator
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    user_list_number_rule_item_operator,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import user_list_prepopulation_status
from airflow.providers.google_vendor.googleads.v12.enums.types import user_list_rule_type
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    user_list_string_rule_item_operator,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={
        "SimilarUserListInfo",
        "CrmBasedUserListInfo",
        "UserListRuleInfo",
        "UserListRuleItemGroupInfo",
        "UserListRuleItemInfo",
        "UserListDateRuleItemInfo",
        "UserListNumberRuleItemInfo",
        "UserListStringRuleItemInfo",
        "CombinedRuleUserListInfo",
        "ExpressionRuleUserListInfo",
        "FlexibleRuleOperandInfo",
        "FlexibleRuleUserListInfo",
        "RuleBasedUserListInfo",
        "LogicalUserListInfo",
        "UserListLogicalRuleInfo",
        "LogicalUserListOperandInfo",
        "BasicUserListInfo",
        "UserListActionInfo",
    },
)


class SimilarUserListInfo(proto.Message):
    r"""SimilarUserList is a list of users which are similar to users
    from another UserList. These lists are read-only and
    automatically created by Google.

    Attributes:
        seed_user_list (str):
            Seed UserList from which this list is
            derived.

            This field is a member of `oneof`_ ``_seed_user_list``.
    """

    seed_user_list = proto.Field(proto.STRING, number=2, optional=True,)


class CrmBasedUserListInfo(proto.Message):
    r"""UserList of CRM users provided by the advertiser.

    Attributes:
        app_id (str):
            A string that uniquely identifies a mobile
            application from which the data was collected.
            For iOS, the ID string is the 9 digit string
            that appears at the end of an App Store URL (for
            example, "476943146" for "Flood-It! 2" whose App
            Store link is
            http://itunes.apple.com/us/app/flood-it!-2/id476943146).
            For Android, the ID string is the application's
            package name (for example,
            "com.labpixies.colordrips" for "Color Drips"
            given Google Play link
            https://play.google.com/store/apps/details?id=com.labpixies.colordrips).
            Required when creating CrmBasedUserList for
            uploading mobile advertising IDs.

            This field is a member of `oneof`_ ``_app_id``.
        upload_key_type (google.ads.googleads.v12.enums.types.CustomerMatchUploadKeyTypeEnum.CustomerMatchUploadKeyType):
            Matching key type of the list.
            Mixed data types are not allowed on the same
            list. This field is required for an ADD
            operation.
        data_source_type (google.ads.googleads.v12.enums.types.UserListCrmDataSourceTypeEnum.UserListCrmDataSourceType):
            Data source of the list. Default value is FIRST_PARTY. Only
            customers on the allow-list can create third-party sourced
            CRM lists.
    """

    app_id = proto.Field(proto.STRING, number=4, optional=True,)
    upload_key_type = proto.Field(
        proto.ENUM,
        number=2,
        enum=customer_match_upload_key_type.CustomerMatchUploadKeyTypeEnum.CustomerMatchUploadKeyType,
    )
    data_source_type = proto.Field(
        proto.ENUM,
        number=3,
        enum=user_list_crm_data_source_type.UserListCrmDataSourceTypeEnum.UserListCrmDataSourceType,
    )


class UserListRuleInfo(proto.Message):
    r"""A client defined rule based on custom parameters sent by web
    sites or uploaded by the advertiser.

    Attributes:
        rule_type (google.ads.googleads.v12.enums.types.UserListRuleTypeEnum.UserListRuleType):
            Rule type is used to determine how to group
            rule items.
            The default is OR of ANDs (disjunctive normal
            form). That is, rule items will be ANDed
            together within rule item groups and the groups
            themselves will be ORed together.

            Currently AND of ORs (conjunctive normal form)
            is only supported for ExpressionRuleUserList.
        rule_item_groups (Sequence[google.ads.googleads.v12.common.types.UserListRuleItemGroupInfo]):
            List of rule item groups that defines this rule. Rule item
            groups are grouped together based on rule_type.
    """

    rule_type = proto.Field(
        proto.ENUM,
        number=1,
        enum=user_list_rule_type.UserListRuleTypeEnum.UserListRuleType,
    )
    rule_item_groups = proto.RepeatedField(
        proto.MESSAGE, number=2, message="UserListRuleItemGroupInfo",
    )


class UserListRuleItemGroupInfo(proto.Message):
    r"""A group of rule items.

    Attributes:
        rule_items (Sequence[google.ads.googleads.v12.common.types.UserListRuleItemInfo]):
            Rule items that will be grouped together based on rule_type.
    """

    rule_items = proto.RepeatedField(
        proto.MESSAGE, number=1, message="UserListRuleItemInfo",
    )


class UserListRuleItemInfo(proto.Message):
    r"""An atomic rule item.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        name (str):
            Rule variable name. It should match the corresponding key
            name fired by the pixel. A name must begin with US-ascii
            letters or underscore or UTF8 code that is greater than 127
            and consist of US-ascii letters or digits or underscore or
            UTF8 code that is greater than 127. For websites, there are
            two built-in variable URL (name = 'url__') and referrer URL
            (name = 'ref_url__'). This field must be populated when
            creating a new rule item.

            This field is a member of `oneof`_ ``_name``.
        number_rule_item (google.ads.googleads.v12.common.types.UserListNumberRuleItemInfo):
            An atomic rule item composed of a number
            operation.

            This field is a member of `oneof`_ ``rule_item``.
        string_rule_item (google.ads.googleads.v12.common.types.UserListStringRuleItemInfo):
            An atomic rule item composed of a string
            operation.

            This field is a member of `oneof`_ ``rule_item``.
        date_rule_item (google.ads.googleads.v12.common.types.UserListDateRuleItemInfo):
            An atomic rule item composed of a date
            operation.

            This field is a member of `oneof`_ ``rule_item``.
    """

    name = proto.Field(proto.STRING, number=5, optional=True,)
    number_rule_item = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="rule_item",
        message="UserListNumberRuleItemInfo",
    )
    string_rule_item = proto.Field(
        proto.MESSAGE,
        number=3,
        oneof="rule_item",
        message="UserListStringRuleItemInfo",
    )
    date_rule_item = proto.Field(
        proto.MESSAGE,
        number=4,
        oneof="rule_item",
        message="UserListDateRuleItemInfo",
    )


class UserListDateRuleItemInfo(proto.Message):
    r"""A rule item composed of a date operation.

    Attributes:
        operator (google.ads.googleads.v12.enums.types.UserListDateRuleItemOperatorEnum.UserListDateRuleItemOperator):
            Date comparison operator.
            This field is required and must be populated
            when creating new date rule item.
        value (str):
            String representing date value to be compared
            with the rule variable. Supported date format is
            YYYY-MM-DD. Times are reported in the customer's
            time zone.

            This field is a member of `oneof`_ ``_value``.
        offset_in_days (int):
            The relative date value of the right hand
            side denoted by number of days offset from now.
            The value field will override this field when
            both are present.

            This field is a member of `oneof`_ ``_offset_in_days``.
    """

    operator = proto.Field(
        proto.ENUM,
        number=1,
        enum=user_list_date_rule_item_operator.UserListDateRuleItemOperatorEnum.UserListDateRuleItemOperator,
    )
    value = proto.Field(proto.STRING, number=4, optional=True,)
    offset_in_days = proto.Field(proto.INT64, number=5, optional=True,)


class UserListNumberRuleItemInfo(proto.Message):
    r"""A rule item composed of a number operation.

    Attributes:
        operator (google.ads.googleads.v12.enums.types.UserListNumberRuleItemOperatorEnum.UserListNumberRuleItemOperator):
            Number comparison operator.
            This field is required and must be populated
            when creating a new number rule item.
        value (float):
            Number value to be compared with the
            variable. This field is required and must be
            populated when creating a new number rule item.

            This field is a member of `oneof`_ ``_value``.
    """

    operator = proto.Field(
        proto.ENUM,
        number=1,
        enum=user_list_number_rule_item_operator.UserListNumberRuleItemOperatorEnum.UserListNumberRuleItemOperator,
    )
    value = proto.Field(proto.DOUBLE, number=3, optional=True,)


class UserListStringRuleItemInfo(proto.Message):
    r"""A rule item composed of a string operation.

    Attributes:
        operator (google.ads.googleads.v12.enums.types.UserListStringRuleItemOperatorEnum.UserListStringRuleItemOperator):
            String comparison operator.
            This field is required and must be populated
            when creating a new string rule item.
        value (str):
            The right hand side of the string rule item.
            For URLs or referrer URLs, the value can not
            contain illegal URL chars such as newlines,
            quotes, tabs, or parentheses. This field is
            required and must be populated when creating a
            new string rule item.

            This field is a member of `oneof`_ ``_value``.
    """

    operator = proto.Field(
        proto.ENUM,
        number=1,
        enum=user_list_string_rule_item_operator.UserListStringRuleItemOperatorEnum.UserListStringRuleItemOperator,
    )
    value = proto.Field(proto.STRING, number=3, optional=True,)


class CombinedRuleUserListInfo(proto.Message):
    r"""User lists defined by combining two rules, left operand and right
    operand. There are two operators: AND where left operand and right
    operand have to be true; AND_NOT where left operand is true but
    right operand is false.

    Attributes:
        left_operand (google.ads.googleads.v12.common.types.UserListRuleInfo):
            Left operand of the combined rule.
            This field is required and must be populated
            when creating new combined rule based user list.
        right_operand (google.ads.googleads.v12.common.types.UserListRuleInfo):
            Right operand of the combined rule.
            This field is required and must be populated
            when creating new combined rule based user list.
        rule_operator (google.ads.googleads.v12.enums.types.UserListCombinedRuleOperatorEnum.UserListCombinedRuleOperator):
            Operator to connect the two operands.
            Required for creating a combined rule user list.
    """

    left_operand = proto.Field(
        proto.MESSAGE, number=1, message="UserListRuleInfo",
    )
    right_operand = proto.Field(
        proto.MESSAGE, number=2, message="UserListRuleInfo",
    )
    rule_operator = proto.Field(
        proto.ENUM,
        number=3,
        enum=user_list_combined_rule_operator.UserListCombinedRuleOperatorEnum.UserListCombinedRuleOperator,
    )


class ExpressionRuleUserListInfo(proto.Message):
    r"""Visitors of a page. The page visit is defined by one boolean
    rule expression.

    Attributes:
        rule (google.ads.googleads.v12.common.types.UserListRuleInfo):
            Boolean rule that defines this user list. The rule consists
            of a list of rule item groups and each rule item group
            consists of a list of rule items. All the rule item groups
            are ORed or ANDed together for evaluation based on
            rule.rule_type.

            Required for creating an expression rule user list.
    """

    rule = proto.Field(proto.MESSAGE, number=1, message="UserListRuleInfo",)


class FlexibleRuleOperandInfo(proto.Message):
    r"""Flexible rule that wraps the common rule and a lookback
    window.

    Attributes:
        rule (google.ads.googleads.v12.common.types.UserListRuleInfo):
            List of rule item groups that defines this
            rule. Rule item groups are grouped together.
        lookback_window_days (int):
            Lookback window for this rule in days. From
            now until X days ago.

            This field is a member of `oneof`_ ``_lookback_window_days``.
    """

    rule = proto.Field(proto.MESSAGE, number=1, message="UserListRuleInfo",)
    lookback_window_days = proto.Field(proto.INT64, number=2, optional=True,)


class FlexibleRuleUserListInfo(proto.Message):
    r"""Flexible rule representation of visitors with one or multiple
    actions.

    Attributes:
        inclusive_rule_operator (google.ads.googleads.v12.enums.types.UserListFlexibleRuleOperatorEnum.UserListFlexibleRuleOperator):
            Operator that defines how the inclusive
            operands are combined.
        inclusive_operands (Sequence[google.ads.googleads.v12.common.types.FlexibleRuleOperandInfo]):
            Actions that are located on the inclusive side. These are
            joined together by either AND/OR as specified by the
            inclusive_rule_operator.
        exclusive_operands (Sequence[google.ads.googleads.v12.common.types.FlexibleRuleOperandInfo]):
            Actions that are located on the exclusive
            side. These are joined together with OR.
    """

    inclusive_rule_operator = proto.Field(
        proto.ENUM,
        number=1,
        enum=user_list_flexible_rule_operator.UserListFlexibleRuleOperatorEnum.UserListFlexibleRuleOperator,
    )
    inclusive_operands = proto.RepeatedField(
        proto.MESSAGE, number=2, message="FlexibleRuleOperandInfo",
    )
    exclusive_operands = proto.RepeatedField(
        proto.MESSAGE, number=3, message="FlexibleRuleOperandInfo",
    )


class RuleBasedUserListInfo(proto.Message):
    r"""Representation of a userlist that is generated by a rule.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        prepopulation_status (google.ads.googleads.v12.enums.types.UserListPrepopulationStatusEnum.UserListPrepopulationStatus):
            The status of pre-population. The field is
            default to NONE if not set which means the
            previous users will not be considered. If set to
            REQUESTED, past site visitors or app users who
            match the list definition will be included in
            the list (works on the Display Network only).
            This will only add past users from within the
            last 30 days, depending on the list's membership
            duration and the date when the remarketing tag
            is added. The status will be updated to FINISHED
            once request is processed, or FAILED if the
            request fails.
        flexible_rule_user_list (google.ads.googleads.v12.common.types.FlexibleRuleUserListInfo):
            Flexible rule representation of visitors with
            one or multiple actions.
        combined_rule_user_list (google.ads.googleads.v12.common.types.CombinedRuleUserListInfo):
            User lists defined by combining two rules. There are two
            operators: AND, where the left and right operands have to be
            true; AND_NOT where left operand is true but right operand
            is false.

            This field is a member of `oneof`_ ``rule_based_user_list``.
        expression_rule_user_list (google.ads.googleads.v12.common.types.ExpressionRuleUserListInfo):
            Visitors of a page. The page visit is defined
            by one boolean rule expression.

            This field is a member of `oneof`_ ``rule_based_user_list``.
    """

    prepopulation_status = proto.Field(
        proto.ENUM,
        number=1,
        enum=user_list_prepopulation_status.UserListPrepopulationStatusEnum.UserListPrepopulationStatus,
    )
    flexible_rule_user_list = proto.Field(
        proto.MESSAGE, number=5, message="FlexibleRuleUserListInfo",
    )
    combined_rule_user_list = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="rule_based_user_list",
        message="CombinedRuleUserListInfo",
    )
    expression_rule_user_list = proto.Field(
        proto.MESSAGE,
        number=4,
        oneof="rule_based_user_list",
        message="ExpressionRuleUserListInfo",
    )


class LogicalUserListInfo(proto.Message):
    r"""Represents a user list that is a custom combination of user
    lists.

    Attributes:
        rules (Sequence[google.ads.googleads.v12.common.types.UserListLogicalRuleInfo]):
            Logical list rules that define this user
            list. The rules are defined as a logical
            operator (ALL/ANY/NONE) and a list of user
            lists. All the rules are ANDed when they are
            evaluated.

            Required for creating a logical user list.
    """

    rules = proto.RepeatedField(
        proto.MESSAGE, number=1, message="UserListLogicalRuleInfo",
    )


class UserListLogicalRuleInfo(proto.Message):
    r"""A user list logical rule. A rule has a logical operator
    (and/or/not) and a list of user lists as operands.

    Attributes:
        operator (google.ads.googleads.v12.enums.types.UserListLogicalRuleOperatorEnum.UserListLogicalRuleOperator):
            The logical operator of the rule.
        rule_operands (Sequence[google.ads.googleads.v12.common.types.LogicalUserListOperandInfo]):
            The list of operands of the rule.
    """

    operator = proto.Field(
        proto.ENUM,
        number=1,
        enum=user_list_logical_rule_operator.UserListLogicalRuleOperatorEnum.UserListLogicalRuleOperator,
    )
    rule_operands = proto.RepeatedField(
        proto.MESSAGE, number=2, message="LogicalUserListOperandInfo",
    )


class LogicalUserListOperandInfo(proto.Message):
    r"""Operand of logical user list that consists of a user list.

    Attributes:
        user_list (str):
            Resource name of a user list as an operand.

            This field is a member of `oneof`_ ``_user_list``.
    """

    user_list = proto.Field(proto.STRING, number=2, optional=True,)


class BasicUserListInfo(proto.Message):
    r"""User list targeting as a collection of conversions or
    remarketing actions.

    Attributes:
        actions (Sequence[google.ads.googleads.v12.common.types.UserListActionInfo]):
            Actions associated with this user list.
    """

    actions = proto.RepeatedField(
        proto.MESSAGE, number=1, message="UserListActionInfo",
    )


class UserListActionInfo(proto.Message):
    r"""Represents an action type used for building remarketing user
    lists.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        conversion_action (str):
            A conversion action that's not generated from
            remarketing.

            This field is a member of `oneof`_ ``user_list_action``.
        remarketing_action (str):
            A remarketing action.

            This field is a member of `oneof`_ ``user_list_action``.
    """

    conversion_action = proto.Field(
        proto.STRING, number=3, oneof="user_list_action",
    )
    remarketing_action = proto.Field(
        proto.STRING, number=4, oneof="user_list_action",
    )


__all__ = tuple(sorted(__protobuf__.manifest))
