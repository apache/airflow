
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

Configuration
=============

This section describes how to configure the Informatica provider for Apache Airflow.

Connection Setup
----------------

Create an HTTP connection in Airflow for Informatica EDC:

1. **Connection Type**: ``informatica_edc``
2. **Host**: Your EDC server hostname
3. **Port**: EDC server port (typically 9087)
4. **Schema**: ``https`` or ``http``
5. **Login**: EDC username
6. **Password**: EDC password
7. **Extras**: Add the following JSON:

   .. code-block:: json

       {"security_domain": "your_security_domain"}

Configuration Options
---------------------

Add to your ``airflow.cfg``:

.. code-block:: ini

    [informatica]
    # Disable sending events without uninstalling the Informatica Provider
    disabled = False
    # The connection ID to use when no connection ID is provided
    default_conn_id = informatica_edc_default

Provider Configuration
----------------------

The provider configuration is defined in ``get_provider_info.py`` and includes:

- ``disabled``: Boolean flag to disable the provider without uninstalling
- ``default_conn_id``: Default connection ID for Informatica EDC

SSL and Security
----------------

The connection supports SSL verification control through extras:

.. code-block:: json

    {
        "security_domain": "your_domain",
        "verify_ssl": true
    }

Set ``verify_ssl`` to ``false`` to disable SSL certificate verification.
