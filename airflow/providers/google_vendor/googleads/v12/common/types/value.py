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
    manifest={"Value",},
)


class Value(proto.Message):
    r"""A generic data container.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        boolean_value (bool):
            A boolean.

            This field is a member of `oneof`_ ``value``.
        int64_value (int):
            An int64.

            This field is a member of `oneof`_ ``value``.
        float_value (float):
            A float.

            This field is a member of `oneof`_ ``value``.
        double_value (float):
            A double.

            This field is a member of `oneof`_ ``value``.
        string_value (str):
            A string.

            This field is a member of `oneof`_ ``value``.
    """

    boolean_value = proto.Field(proto.BOOL, number=1, oneof="value",)
    int64_value = proto.Field(proto.INT64, number=2, oneof="value",)
    float_value = proto.Field(proto.FLOAT, number=3, oneof="value",)
    double_value = proto.Field(proto.DOUBLE, number=4, oneof="value",)
    string_value = proto.Field(proto.STRING, number=5, oneof="value",)


__all__ = tuple(sorted(__protobuf__.manifest))
