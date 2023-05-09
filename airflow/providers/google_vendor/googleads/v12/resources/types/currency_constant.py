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
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CurrencyConstant",},
)


class CurrencyConstant(proto.Message):
    r"""A currency constant.

    Attributes:
        resource_name (str):
            Output only. The resource name of the currency constant.
            Currency constant resource names have the form:

            ``currencyConstants/{code}``
        code (str):
            Output only. ISO 4217 three-letter currency
            code, for example, "USD".

            This field is a member of `oneof`_ ``_code``.
        name (str):
            Output only. Full English name of the
            currency.

            This field is a member of `oneof`_ ``_name``.
        symbol (str):
            Output only. Standard symbol for describing
            this currency, for example, '$' for US Dollars.

            This field is a member of `oneof`_ ``_symbol``.
        billable_unit_micros (int):
            Output only. The billable unit for this
            currency. Billed amounts should be multiples of
            this value.

            This field is a member of `oneof`_ ``_billable_unit_micros``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    code = proto.Field(proto.STRING, number=6, optional=True,)
    name = proto.Field(proto.STRING, number=7, optional=True,)
    symbol = proto.Field(proto.STRING, number=8, optional=True,)
    billable_unit_micros = proto.Field(proto.INT64, number=9, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
