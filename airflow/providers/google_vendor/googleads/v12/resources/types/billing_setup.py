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

from airflow.providers.google_vendor.googleads.v12.enums.types import billing_setup_status
from airflow.providers.google_vendor.googleads.v12.enums.types import time_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"BillingSetup",},
)


class BillingSetup(proto.Message):
    r"""A billing setup, which associates a payments account and an
    advertiser. A billing setup is specific to one advertiser.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the billing setup.
            BillingSetup resource names have the form:

            ``customers/{customer_id}/billingSetups/{billing_setup_id}``
        id (int):
            Output only. The ID of the billing setup.

            This field is a member of `oneof`_ ``_id``.
        status (google.ads.googleads.v12.enums.types.BillingSetupStatusEnum.BillingSetupStatus):
            Output only. The status of the billing setup.
        payments_account (str):
            Immutable. The resource name of the payments account
            associated with this billing setup. Payments resource names
            have the form:

            ``customers/{customer_id}/paymentsAccounts/{payments_account_id}``
            When setting up billing, this is used to signup with an
            existing payments account (and then payments_account_info
            should not be set). When getting a billing setup, this and
            payments_account_info will be populated.

            This field is a member of `oneof`_ ``_payments_account``.
        payments_account_info (google.ads.googleads.v12.resources.types.BillingSetup.PaymentsAccountInfo):
            Immutable. The payments account information associated with
            this billing setup. When setting up billing, this is used to
            signup with a new payments account (and then
            payments_account should not be set). When getting a billing
            setup, this and payments_account will be populated.
        start_date_time (str):
            Immutable. The start date time in yyyy-MM-dd
            or yyyy-MM-dd HH:mm:ss format. Only a future
            time is allowed.

            This field is a member of `oneof`_ ``start_time``.
        start_time_type (google.ads.googleads.v12.enums.types.TimeTypeEnum.TimeType):
            Immutable. The start time as a type. Only NOW
            is allowed.

            This field is a member of `oneof`_ ``start_time``.
        end_date_time (str):
            Output only. The end date time in yyyy-MM-dd
            or yyyy-MM-dd HH:mm:ss format.

            This field is a member of `oneof`_ ``end_time``.
        end_time_type (google.ads.googleads.v12.enums.types.TimeTypeEnum.TimeType):
            Output only. The end time as a type.  The
            only possible value is FOREVER.

            This field is a member of `oneof`_ ``end_time``.
    """

    class PaymentsAccountInfo(proto.Message):
        r"""Container of payments account information for this billing.

        Attributes:
            payments_account_id (str):
                Output only. A 16 digit id used to identify
                the payments account associated with the billing
                setup.
                This must be passed as a string with dashes, for
                example, "1234-5678-9012-3456".

                This field is a member of `oneof`_ ``_payments_account_id``.
            payments_account_name (str):
                Immutable. The name of the payments account
                associated with the billing setup.
                This enables the user to specify a meaningful
                name for a payments account to aid in
                reconciling monthly invoices.

                This name will be printed in the monthly
                invoices.

                This field is a member of `oneof`_ ``_payments_account_name``.
            payments_profile_id (str):
                Immutable. A 12 digit id used to identify the
                payments profile associated with the billing
                setup.
                This must be passed in as a string with dashes,
                for example, "1234-5678-9012".

                This field is a member of `oneof`_ ``_payments_profile_id``.
            payments_profile_name (str):
                Output only. The name of the payments profile
                associated with the billing setup.

                This field is a member of `oneof`_ ``_payments_profile_name``.
            secondary_payments_profile_id (str):
                Output only. A secondary payments profile id
                present in uncommon situations, for example,
                when a sequential liability agreement has been
                arranged.

                This field is a member of `oneof`_ ``_secondary_payments_profile_id``.
        """

        payments_account_id = proto.Field(
            proto.STRING, number=6, optional=True,
        )
        payments_account_name = proto.Field(
            proto.STRING, number=7, optional=True,
        )
        payments_profile_id = proto.Field(
            proto.STRING, number=8, optional=True,
        )
        payments_profile_name = proto.Field(
            proto.STRING, number=9, optional=True,
        )
        secondary_payments_profile_id = proto.Field(
            proto.STRING, number=10, optional=True,
        )

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=15, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=3,
        enum=billing_setup_status.BillingSetupStatusEnum.BillingSetupStatus,
    )
    payments_account = proto.Field(proto.STRING, number=18, optional=True,)
    payments_account_info = proto.Field(
        proto.MESSAGE, number=12, message=PaymentsAccountInfo,
    )
    start_date_time = proto.Field(proto.STRING, number=16, oneof="start_time",)
    start_time_type = proto.Field(
        proto.ENUM,
        number=10,
        oneof="start_time",
        enum=time_type.TimeTypeEnum.TimeType,
    )
    end_date_time = proto.Field(proto.STRING, number=17, oneof="end_time",)
    end_time_type = proto.Field(
        proto.ENUM,
        number=14,
        oneof="end_time",
        enum=time_type.TimeTypeEnum.TimeType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
