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

from airflow.providers.google_vendor.googleads.v12.enums.types import budget_delivery_method
from airflow.providers.google_vendor.googleads.v12.enums.types import budget_period
from airflow.providers.google_vendor.googleads.v12.enums.types import budget_status
from airflow.providers.google_vendor.googleads.v12.enums.types import budget_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CampaignBudget",},
)


class CampaignBudget(proto.Message):
    r"""A campaign budget.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the campaign budget.
            Campaign budget resource names have the form:

            ``customers/{customer_id}/campaignBudgets/{campaign_budget_id}``
        id (int):
            Output only. The ID of the campaign budget.
            A campaign budget is created using the
            CampaignBudgetService create operation and is
            assigned a budget ID. A budget ID can be shared
            across different campaigns; the system will then
            allocate the campaign budget among different
            campaigns to get optimum results.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The name of the campaign budget.
            When creating a campaign budget through
            CampaignBudgetService, every explicitly shared
            campaign budget must have a non-null, non-empty
            name. Campaign budgets that are not explicitly
            shared derive their name from the attached
            campaign's name.

            The length of this string must be between 1 and
            255, inclusive, in UTF-8 bytes, (trimmed).

            This field is a member of `oneof`_ ``_name``.
        amount_micros (int):
            The amount of the budget, in the local
            currency for the account. Amount is specified in
            micros, where one million is equivalent to one
            currency unit. Monthly spend is capped at 30.4
            times this amount.

            This field is a member of `oneof`_ ``_amount_micros``.
        total_amount_micros (int):
            The lifetime amount of the budget, in the
            local currency for the account. Amount is
            specified in micros, where one million is
            equivalent to one currency unit.

            This field is a member of `oneof`_ ``_total_amount_micros``.
        status (google.ads.googleads.v12.enums.types.BudgetStatusEnum.BudgetStatus):
            Output only. The status of this campaign
            budget. This field is read-only.
        delivery_method (google.ads.googleads.v12.enums.types.BudgetDeliveryMethodEnum.BudgetDeliveryMethod):
            The delivery method that determines the rate
            at which the campaign budget is spent.

            Defaults to STANDARD if unspecified in a create
            operation.
        explicitly_shared (bool):
            Specifies whether the budget is explicitly
            shared. Defaults to true if unspecified in a
            create operation.
            If true, the budget was created with the purpose
            of sharing across one or more campaigns.

            If false, the budget was created with the
            intention of only being used with a single
            campaign. The budget's name and status will stay
            in sync with the campaign's name and status.
            Attempting to share the budget with a second
            campaign will result in an error.

            A non-shared budget can become an explicitly
            shared. The same operation must also assign the
            budget a name.

            A shared campaign budget can never become
            non-shared.

            This field is a member of `oneof`_ ``_explicitly_shared``.
        reference_count (int):
            Output only. The number of campaigns actively
            using the budget.
            This field is read-only.

            This field is a member of `oneof`_ ``_reference_count``.
        has_recommended_budget (bool):
            Output only. Indicates whether there is a
            recommended budget for this campaign budget.
            This field is read-only.

            This field is a member of `oneof`_ ``_has_recommended_budget``.
        recommended_budget_amount_micros (int):
            Output only. The recommended budget amount.
            If no recommendation is available, this will be
            set to the budget amount. Amount is specified in
            micros, where one million is equivalent to one
            currency unit.

            This field is read-only.

            This field is a member of `oneof`_ ``_recommended_budget_amount_micros``.
        period (google.ads.googleads.v12.enums.types.BudgetPeriodEnum.BudgetPeriod):
            Immutable. Period over which to spend the
            budget. Defaults to DAILY if not specified.
        recommended_budget_estimated_change_weekly_clicks (int):
            Output only. The estimated change in weekly
            clicks if the recommended budget is applied.
            This field is read-only.

            This field is a member of `oneof`_ ``_recommended_budget_estimated_change_weekly_clicks``.
        recommended_budget_estimated_change_weekly_cost_micros (int):
            Output only. The estimated change in weekly
            cost in micros if the recommended budget is
            applied. One million is equivalent to one
            currency unit.
            This field is read-only.

            This field is a member of `oneof`_ ``_recommended_budget_estimated_change_weekly_cost_micros``.
        recommended_budget_estimated_change_weekly_interactions (int):
            Output only. The estimated change in weekly
            interactions if the recommended budget is
            applied.
            This field is read-only.

            This field is a member of `oneof`_ ``_recommended_budget_estimated_change_weekly_interactions``.
        recommended_budget_estimated_change_weekly_views (int):
            Output only. The estimated change in weekly
            views if the recommended budget is applied.
            This field is read-only.

            This field is a member of `oneof`_ ``_recommended_budget_estimated_change_weekly_views``.
        type_ (google.ads.googleads.v12.enums.types.BudgetTypeEnum.BudgetType):
            Immutable. The type of the campaign budget.
        aligned_bidding_strategy_id (int):
            ID of the portfolio bidding strategy that
            this shared campaign budget is aligned with.
            When a bidding strategy and a campaign budget
            are aligned, they are attached to the same set
            of campaigns. After a campaign budget is aligned
            with a bidding strategy, campaigns that are
            added to the campaign budget must also use the
            aligned bidding strategy.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=19, optional=True,)
    name = proto.Field(proto.STRING, number=20, optional=True,)
    amount_micros = proto.Field(proto.INT64, number=21, optional=True,)
    total_amount_micros = proto.Field(proto.INT64, number=22, optional=True,)
    status = proto.Field(
        proto.ENUM, number=6, enum=budget_status.BudgetStatusEnum.BudgetStatus,
    )
    delivery_method = proto.Field(
        proto.ENUM,
        number=7,
        enum=budget_delivery_method.BudgetDeliveryMethodEnum.BudgetDeliveryMethod,
    )
    explicitly_shared = proto.Field(proto.BOOL, number=23, optional=True,)
    reference_count = proto.Field(proto.INT64, number=24, optional=True,)
    has_recommended_budget = proto.Field(proto.BOOL, number=25, optional=True,)
    recommended_budget_amount_micros = proto.Field(
        proto.INT64, number=26, optional=True,
    )
    period = proto.Field(
        proto.ENUM, number=13, enum=budget_period.BudgetPeriodEnum.BudgetPeriod,
    )
    recommended_budget_estimated_change_weekly_clicks = proto.Field(
        proto.INT64, number=27, optional=True,
    )
    recommended_budget_estimated_change_weekly_cost_micros = proto.Field(
        proto.INT64, number=28, optional=True,
    )
    recommended_budget_estimated_change_weekly_interactions = proto.Field(
        proto.INT64, number=29, optional=True,
    )
    recommended_budget_estimated_change_weekly_views = proto.Field(
        proto.INT64, number=30, optional=True,
    )
    type_ = proto.Field(
        proto.ENUM, number=18, enum=budget_type.BudgetTypeEnum.BudgetType,
    )
    aligned_bidding_strategy_id = proto.Field(proto.INT64, number=31,)


__all__ = tuple(sorted(__protobuf__.manifest))
