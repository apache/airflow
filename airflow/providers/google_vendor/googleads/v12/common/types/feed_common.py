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
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={"Money",},
)


class Money(proto.Message):
    r"""Represents a price in a particular currency.

    Attributes:
        currency_code (str):
            Three-character ISO 4217 currency code.

            This field is a member of `oneof`_ ``_currency_code``.
        amount_micros (int):
            Amount in micros. One million is equivalent
            to one unit.

            This field is a member of `oneof`_ ``_amount_micros``.
    """

    currency_code = proto.Field(proto.STRING, number=3, optional=True,)
    amount_micros = proto.Field(proto.INT64, number=4, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
