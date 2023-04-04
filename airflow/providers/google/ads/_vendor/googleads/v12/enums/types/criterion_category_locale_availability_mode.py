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
    package="google.ads.googleads.v12.enums",
    marshal="google.ads.googleads.v12",
    manifest={"CriterionCategoryLocaleAvailabilityModeEnum",},
)


class CriterionCategoryLocaleAvailabilityModeEnum(proto.Message):
    r"""Describes locale availability mode for a criterion
    availability - whether it's available globally, or a particular
    country with all languages, or a particular language with all
    countries, or a country-language pair.

    """

    class CriterionCategoryLocaleAvailabilityMode(proto.Enum):
        r"""Enum containing the possible
        CriterionCategoryLocaleAvailabilityMode.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        ALL_LOCALES = 2
        COUNTRY_AND_ALL_LANGUAGES = 3
        LANGUAGE_AND_ALL_COUNTRIES = 4
        COUNTRY_AND_LANGUAGE = 5


__all__ = tuple(sorted(__protobuf__.manifest))
