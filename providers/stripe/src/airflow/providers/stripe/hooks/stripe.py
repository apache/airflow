#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import stripe

from airflow.providers.common.compat.sdk import AirflowException, BaseHook

if TYPE_CHECKING:
    from stripe import StripeObject


class StripeHook(BaseHook):
    """
    Hook for interacting with the Stripe API.

    Uses the official ``stripe`` Python SDK. The API key is read from the
    **Password** field of the Airflow connection. An optional Stripe API
    version can be stored in the connection's **Extra** field under the key
    ``stripe_version``.

    :param stripe_conn_id: Airflow connection ID for Stripe credentials.
    """

    conn_name_attr = "stripe_conn_id"
    default_conn_name = "stripe_default"
    conn_type = "stripe"
    hook_name = "Stripe"

    def __init__(self, stripe_conn_id: str = "stripe_default") -> None:
        super().__init__()
        self.stripe_conn_id = stripe_conn_id
        self._client: stripe.StripeClient | None = None

    def _get_api_key(self) -> str:
        conn = self.get_connection(self.stripe_conn_id)
        api_key = conn.password
        if not api_key:
            raise AirflowException(
                "No Stripe API key found. Set the Password field of the "
                f"'{self.stripe_conn_id}' connection to your Stripe secret key."
            )
        return api_key

    def _get_stripe_version(self) -> str | None:
        conn = self.get_connection(self.stripe_conn_id)
        return conn.extra_dejson.get("stripe_version")

    def get_client(self) -> stripe.StripeClient:
        """Return an authenticated :class:`stripe.StripeClient` instance."""
        if self._client is None:
            api_key = self._get_api_key()
            stripe_version = self._get_stripe_version()
            kwargs: dict[str, Any] = {}
            if stripe_version:
                kwargs["stripe_version"] = stripe_version
            self._client = stripe.StripeClient(api_key, **kwargs)
        return self._client

    def list_all(self, resource: str, **list_params: Any) -> list[StripeObject]:
        """
        Retrieve all objects of a given Stripe resource, handling pagination automatically.

        Uses the ``auto_paging_iter()`` iterator provided by the Stripe SDK so
        that large result sets are fetched page by page without loading everything
        into memory at once before returning.

        :param resource: Stripe resource name as it appears on the client, e.g.
            ``"Customer"``, ``"Subscription"``, ``"Invoice"``, ``"Event"``.
        :param list_params: Additional keyword arguments passed directly to the
            Stripe ``list`` call (e.g. ``limit=100``, ``created={"gte": ts}``).
        :returns: A list of Stripe objects.
        :raises AirflowException: If the resource is not found on the client.

        Example::

            hook = StripeHook()
            customers = hook.list_all("Customer", limit=100)
        """
        client = self.get_client()
        resource_attr = resource.lower() + "s"
        resource_client = getattr(client, resource_attr, None)
        if resource_client is None:
            raise AirflowException(
                f"Stripe client has no resource '{resource_attr}'. "
                f"Check the resource name passed to list_all()."
            )
        results: list[StripeObject] = []
        for obj in resource_client.list(list_params).auto_paging_iter():
            results.append(obj)
        return results

    def retrieve(self, resource: str, sid: str, **params: Any) -> StripeObject:
        """
        Retrieve a single Stripe object by ID.

        :param resource: Stripe resource name, e.g. ``"Customer"``.
        :param sid: The Stripe object ID (e.g. ``"cus_..."``, ``"sub_..."``).
        :param params: Additional keyword arguments passed to the ``retrieve`` call.
        :returns: The Stripe object.
        :raises AirflowException: If the resource is not found on the client.
        """
        client = self.get_client()
        resource_attr = resource.lower() + "s"
        resource_client = getattr(client, resource_attr, None)
        if resource_client is None:
            raise AirflowException(
                f"Stripe client has no resource '{resource_attr}'. "
                f"Check the resource name passed to retrieve()."
            )
        return resource_client.retrieve(sid, params)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to the connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "stripe_version": StringField(
                lazy_gettext("Stripe API Version"),
                widget=BS3TextFieldWidget(),
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour for the connection UI."""
        return {
            "hidden_fields": ["schema", "login", "host", "port", "extra"],
            "relabeling": {"password": "API Key (Secret Key)"},
            "placeholders": {"password": "sk_live_..."},
        }
