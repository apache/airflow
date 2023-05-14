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

from airflow.providers.google_vendor.googleads.v12.enums.types import manager_link_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CustomerClientLink",},
)


class CustomerClientLink(proto.Message):
    r"""Represents customer client link relationship.

    Attributes:
        resource_name (str):
            Immutable. Name of the resource. CustomerClientLink resource
            names have the form:
            ``customers/{customer_id}/customerClientLinks/{client_customer_id}~{manager_link_id}``
        client_customer (str):
            Immutable. The client customer linked to this
            customer.

            This field is a member of `oneof`_ ``_client_customer``.
        manager_link_id (int):
            Output only. This is uniquely identifies a
            customer client link. Read only.

            This field is a member of `oneof`_ ``_manager_link_id``.
        status (google.ads.googleads.v12.enums.types.ManagerLinkStatusEnum.ManagerLinkStatus):
            This is the status of the link between client
            and manager.
        hidden (bool):
            The visibility of the link. Users can choose
            whether or not to see hidden links in the Google
            Ads UI. Default value is false

            This field is a member of `oneof`_ ``_hidden``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    client_customer = proto.Field(proto.STRING, number=7, optional=True,)
    manager_link_id = proto.Field(proto.INT64, number=8, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=manager_link_status.ManagerLinkStatusEnum.ManagerLinkStatus,
    )
    hidden = proto.Field(proto.BOOL, number=9, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
