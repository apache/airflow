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


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.errors",
    marshal="google.ads.googleads.v12",
    manifest={"InvoiceErrorEnum",},
)


class InvoiceErrorEnum(proto.Message):
    r"""Container for enum describing possible invoice errors.
    """

    class InvoiceError(proto.Enum):
        r"""Enum describing possible invoice errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        YEAR_MONTH_TOO_OLD = 2
        NOT_INVOICED_CUSTOMER = 3
        BILLING_SETUP_NOT_APPROVED = 4
        BILLING_SETUP_NOT_ON_MONTHLY_INVOICING = 5
        NON_SERVING_CUSTOMER = 6


__all__ = tuple(sorted(__protobuf__.manifest))
