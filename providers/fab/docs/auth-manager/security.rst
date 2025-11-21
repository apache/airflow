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

FAB auth manager UI security options
====================================

.. note::
    This guide only applies to FAB auth manager UI pages. These pages are accessible under category "Security" in the
    menu.

Sensitive Variable fields
-------------------------

Variable values that are deemed "sensitive" based on the variable name will be masked in the UI automatically.
See :ref:`security:mask-sensitive-values` for more details.

SSL
---

SSL can be enabled by providing a certificate and key. Once enabled, be sure to use
"https://" in your browser.

.. code-block:: ini

    [webserver]
    web_server_ssl_cert = <path to cert>
    web_server_ssl_key = <path to key>

Enabling SSL will not automatically change the web server port. If you want to use the
standard port 443, you'll need to configure that too. Be aware that super user privileges
(or cap_net_bind_service on Linux) are required to listen on port 443.

.. code-block:: ini

    # Optionally, set the server to listen on the standard SSL port.
    web_server_port = 443
    base_url = http://<hostname or IP>:443

Enable CeleryExecutor with SSL. Ensure you properly generate client and server
certs and keys.

.. code-block:: ini

    [celery]
    ssl_active = True
    ssl_key = <path to key>
    ssl_cert = <path to cert>
    ssl_cacert = <path to cacert>

Rate limiting
-------------

Airflow can be configured to limit the number of authentication requests in a given time window. We are using
`Flask-Limiter <https://flask-limiter.readthedocs.io/en/stable/>`_ to achieve that and by default Airflow
uses per-webserver default limit of 5 requests per 40 second fixed window. By default no common storage for
rate limits is used between the gunicorn processes you run so rate-limit is applied separately for each process,
so assuming random distribution of the requests by gunicorn with single webserver instance and default 4
gunicorn workers, the effective rate limit is 5 x 4 = 20 requests per 40 second window (more or less).

However, you can configure the rate limit to be shared between processes by using a common storage backend like Redis.

Example (using airflow.cfg):
.. code-block:: ini

    [fab]
    auth_rate_limit_storage_uri = redis://redis_host:6379/0
    auth_rate_limit_storage_options = {"socket_timeout": 5}

Example (using environment variables):
.. code-block:: bash

    export AIRFLOW__FAB__AUTH_RATE_LIMIT_STORAGE_URI="redis://redis_host:6379/0"
    export AIRFLOW__FAB__AUTH_RATE_LIMIT_STORAGE_OPTIONS='{"socket_timeout": 5}'

Legacy Method:
Alternatively, these settings can be configured by adding them directly to your webserver_config.py file

.. code-block:: python

    RATELIMIT_STORAGE_URI = "redis://redis_host:6379/0"

You can also configure other rate limit settings in ``webserver_config.py`` - for more details, see the
`Flask Limiter rate limit configuration <https://flask-limiter.readthedocs.io/en/stable/configuration.html>`_.
