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

from airflow.providers.google_vendor.googleads.v12.enums.types import customer_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CustomerClient",},
)


class CustomerClient(proto.Message):
    r"""A link between the given customer and a client customer.
    CustomerClients only exist for manager customers. All direct and
    indirect client customers are included, as well as the manager
    itself.

    Attributes:
        resource_name (str):
            Output only. The resource name of the customer client.
            CustomerClient resource names have the form:
            ``customers/{customer_id}/customerClients/{client_customer_id}``
        client_customer (str):
            Output only. The resource name of the
            client-customer which is linked to the given
            customer. Read only.

            This field is a member of `oneof`_ ``_client_customer``.
        hidden (bool):
            Output only. Specifies whether this is a `hidden
            account <https://support.google.com/google-ads/answer/7519830>`__.
            Read only.

            This field is a member of `oneof`_ ``_hidden``.
        level (int):
            Output only. Distance between given customer
            and client. For self link, the level value will
            be 0. Read only.

            This field is a member of `oneof`_ ``_level``.
        time_zone (str):
            Output only. Common Locale Data Repository (CLDR) string
            representation of the time zone of the client, for example,
            America/Los_Angeles. Read only.

            This field is a member of `oneof`_ ``_time_zone``.
        test_account (bool):
            Output only. Identifies if the client is a
            test account. Read only.

            This field is a member of `oneof`_ ``_test_account``.
        manager (bool):
            Output only. Identifies if the client is a
            manager. Read only.

            This field is a member of `oneof`_ ``_manager``.
        descriptive_name (str):
            Output only. Descriptive name for the client.
            Read only.

            This field is a member of `oneof`_ ``_descriptive_name``.
        currency_code (str):
            Output only. Currency code (for example,
            'USD', 'EUR') for the client. Read only.

            This field is a member of `oneof`_ ``_currency_code``.
        id (int):
            Output only. The ID of the client customer.
            Read only.

            This field is a member of `oneof`_ ``_id``.
        applied_labels (Sequence[str]):
            Output only. The resource names of the labels owned by the
            requesting customer that are applied to the client customer.
            Label resource names have the form:

            ``customers/{customer_id}/labels/{label_id}``
        status (google.ads.googleads.v12.enums.types.CustomerStatusEnum.CustomerStatus):
            Output only. The status of the client
            customer. Read only.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    client_customer = proto.Field(proto.STRING, number=12, optional=True,)
    hidden = proto.Field(proto.BOOL, number=13, optional=True,)
    level = proto.Field(proto.INT64, number=14, optional=True,)
    time_zone = proto.Field(proto.STRING, number=15, optional=True,)
    test_account = proto.Field(proto.BOOL, number=16, optional=True,)
    manager = proto.Field(proto.BOOL, number=17, optional=True,)
    descriptive_name = proto.Field(proto.STRING, number=18, optional=True,)
    currency_code = proto.Field(proto.STRING, number=19, optional=True,)
    id = proto.Field(proto.INT64, number=20, optional=True,)
    applied_labels = proto.RepeatedField(proto.STRING, number=21,)
    status = proto.Field(
        proto.ENUM,
        number=22,
        enum=customer_status.CustomerStatusEnum.CustomerStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
