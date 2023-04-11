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

from airflow.providers.google_vendor.googleads.v12.common.types import tag_snippet


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"RemarketingAction",},
)


class RemarketingAction(proto.Message):
    r"""A remarketing action. A snippet of JavaScript code that will
    collect the product id and the type of page people visited
    (product page, shopping cart page, purchase page, general site
    visit) on an advertiser's website.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the remarketing action.
            Remarketing action resource names have the form:

            ``customers/{customer_id}/remarketingActions/{remarketing_action_id}``
        id (int):
            Output only. Id of the remarketing action.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The name of the remarketing action.
            This field is required and should not be empty
            when creating new remarketing actions.

            This field is a member of `oneof`_ ``_name``.
        tag_snippets (Sequence[google.ads.googleads.v12.common.types.TagSnippet]):
            Output only. The snippets used for tracking
            remarketing actions.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=5, optional=True,)
    name = proto.Field(proto.STRING, number=6, optional=True,)
    tag_snippets = proto.RepeatedField(
        proto.MESSAGE, number=4, message=tag_snippet.TagSnippet,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
