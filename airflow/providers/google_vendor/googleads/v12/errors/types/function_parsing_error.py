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
    manifest={"FunctionParsingErrorEnum",},
)


class FunctionParsingErrorEnum(proto.Message):
    r"""Container for enum describing possible function parsing
    errors.

    """

    class FunctionParsingError(proto.Enum):
        r"""Enum describing possible function parsing errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        NO_MORE_INPUT = 2
        EXPECTED_CHARACTER = 3
        UNEXPECTED_SEPARATOR = 4
        UNMATCHED_LEFT_BRACKET = 5
        UNMATCHED_RIGHT_BRACKET = 6
        TOO_MANY_NESTED_FUNCTIONS = 7
        MISSING_RIGHT_HAND_OPERAND = 8
        INVALID_OPERATOR_NAME = 9
        FEED_ATTRIBUTE_OPERAND_ARGUMENT_NOT_INTEGER = 10
        NO_OPERANDS = 11
        TOO_MANY_OPERANDS = 12


__all__ = tuple(sorted(__protobuf__.manifest))
