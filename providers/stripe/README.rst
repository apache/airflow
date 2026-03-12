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

``apache-airflow-providers-stripe``
=====================================

`Stripe <https://stripe.com/>`__

This package provides an ``apache-airflow`` provider for interacting with the
`Stripe API <https://stripe.com/docs/api>`__.

Installation
------------

You can install this package on top of an existing Airflow installation via:

.. code-block:: bash

    pip install apache-airflow-providers-stripe

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

Configuration
-------------

Create an Airflow connection of type ``stripe`` with your Stripe secret key
stored in the **Password** field.

.. code-block:: json

    {
      "conn_type": "stripe",
      "password": "sk_live_..."
    }

Usage
-----

.. code-block:: python

    from airflow.providers.stripe.hooks.stripe import StripeHook

    hook = StripeHook(stripe_conn_id="stripe_default")
    customers = hook.list_all("Customer")

For more information, see the
`Airflow documentation <https://airflow.apache.org/docs/apache-airflow-providers-stripe/>`__.
