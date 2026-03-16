 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _howto/connection:stripe:

Stripe Connection
=================

The Stripe connection type enables integrations with the `Stripe API <https://stripe.com/docs/api>`__.


Default Connection IDs
----------------------

The :class:`~airflow.providers.stripe.hooks.stripe.StripeHook` uses ``stripe_default`` by default.


Configuring the Connection
--------------------------

Password
    Your Stripe **secret key** (``sk_live_...`` for production, ``sk_test_...`` for test mode).
    This field is required.

Extra (optional)
    A JSON dictionary with optional parameters:

    ``stripe_version``: Pin a specific
    `Stripe API version <https://stripe.com/docs/api/versioning>`__
    (e.g. ``"2023-10-16"``). When omitted, the default version of your
    Stripe account is used.


Secret Management
-----------------

When storing the connection in a secret manager, use the default connection name::

  secret name: airflow/connections/stripe_default

Example secret contents:

.. code-block:: json

    {
      "conn_type": "stripe",
      "password": "sk_live_...",
      "extra": {
        "stripe_version": "2023-10-16"
      }
    }
