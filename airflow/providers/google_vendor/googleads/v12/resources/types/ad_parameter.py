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
    manifest={"AdParameter",},
)


class AdParameter(proto.Message):
    r"""An ad parameter that is used to update numeric values (such as
    prices or inventory levels) in any text line of an ad (including
    URLs). There can be a maximum of two AdParameters per ad group
    criterion. (One with parameter_index = 1 and one with
    parameter_index = 2.) In the ad the parameters are referenced by a
    placeholder of the form "{param#:value}". For example,
    "{param1:$17}"

    Attributes:
        resource_name (str):
            Immutable. The resource name of the ad parameter. Ad
            parameter resource names have the form:

            ``customers/{customer_id}/adParameters/{ad_group_id}~{criterion_id}~{parameter_index}``
        ad_group_criterion (str):
            Immutable. The ad group criterion that this
            ad parameter belongs to.

            This field is a member of `oneof`_ ``_ad_group_criterion``.
        parameter_index (int):
            Immutable. The unique index of this ad
            parameter. Must be either 1 or 2.

            This field is a member of `oneof`_ ``_parameter_index``.
        insertion_text (str):
            Numeric value to insert into the ad text. The
            following restrictions  apply:
             - Can use comma or period as a separator, with
            an optional period or    comma (respectively)
            for fractional values. For example, 1,000,000.00
            and 2.000.000,10 are valid.
             - Can be prepended or appended with a currency
            symbol. For example,    $99.99 is valid.
             - Can be prepended or appended with a currency
            code. For example, 99.99USD    and EUR200 are
            valid.
             - Can use '%'. For example, 1.0% and 1,0% are
            valid.  - Can use plus or minus. For example,
            -10.99 and 25+ are valid.  - Can use '/' between
            two numbers. For example 4/1 and 0.95/0.45 are
            valid.

            This field is a member of `oneof`_ ``_insertion_text``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    ad_group_criterion = proto.Field(proto.STRING, number=5, optional=True,)
    parameter_index = proto.Field(proto.INT64, number=6, optional=True,)
    insertion_text = proto.Field(proto.STRING, number=7, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
