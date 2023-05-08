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

from airflow.providers.google_vendor.googleads.v12.enums.types import month_of_year
from airflow.providers.google_vendor.googleads.v12.resources.types import invoice


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={"ListInvoicesRequest", "ListInvoicesResponse",},
)


class ListInvoicesRequest(proto.Message):
    r"""Request message for fetching the invoices of a given billing
    setup that were issued during a given month.

    Attributes:
        customer_id (str):
            Required. The ID of the customer to fetch
            invoices for.
        billing_setup (str):
            Required. The billing setup resource name of the requested
            invoices.

            ``customers/{customer_id}/billingSetups/{billing_setup_id}``
        issue_year (str):
            Required. The issue year to retrieve
            invoices, in yyyy format. Only invoices issued
            in 2019 or later can be retrieved.
        issue_month (google.ads.googleads.v12.enums.types.MonthOfYearEnum.MonthOfYear):
            Required. The issue month to retrieve
            invoices.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    billing_setup = proto.Field(proto.STRING, number=2,)
    issue_year = proto.Field(proto.STRING, number=3,)
    issue_month = proto.Field(
        proto.ENUM, number=4, enum=month_of_year.MonthOfYearEnum.MonthOfYear,
    )


class ListInvoicesResponse(proto.Message):
    r"""Response message for
    [InvoiceService.ListInvoices][google.ads.googleads.v12.services.InvoiceService.ListInvoices].

    Attributes:
        invoices (Sequence[google.ads.googleads.v12.resources.types.Invoice]):
            The list of invoices that match the billing
            setup and time period.
    """

    invoices = proto.RepeatedField(
        proto.MESSAGE, number=1, message=invoice.Invoice,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
