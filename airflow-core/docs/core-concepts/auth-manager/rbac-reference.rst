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

RBAC reference
==============

Auth managers like the Keycloak auth manager need to model the Airflow RBAC
permissions in an external system (for example Keycloak clients). To avoid
hand-maintaining a copy of that model,
``BaseAuthManager.get_rbac_reference()`` returns a machine-readable reference
of every resource the auth manager authorizes. The reference is introspected
from the ``is_authorized_*`` methods, so it stays in sync automatically
whenever resources are added or changed. Call it at runtime for the
authoritative reference rather than copying values from this page:

.. code-block:: python

    from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager

    reference = BaseAuthManager.get_rbac_reference()
    # {"asset": {"method": "is_authorized_asset", "actions": ["GET", "POST", ...], ...}, ...}

Each entry is keyed by resource name (for example ``"dag"``) and holds:

* ``method``: the ``is_authorized_*`` method that authorizes the resource.
* ``actions``: the allowed actions (``GET``, ``POST``, ``PUT``, ``DELETE``).
  Absent for resources authorized without an HTTP method, such as views.
* ``scope``: the enum that further scopes the check, when present (for example
  ``DagAccessEntity`` for Dags, ``AccessView`` for views), with its values.
* ``details``: the details dataclass and its fields, when the request carries
  details (for example ``DagDetails`` with ``id`` and ``team_name``).
* ``description``: a short description of the resource.

See :doc:`index` for the full list of authorization methods and their semantics.
