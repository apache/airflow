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

Webserver
=========

This topic describes how to configure Airflow to secure your webserver.

Rendering Airflow UI in a Web Frame from another site
------------------------------------------------------

Using Airflow in a web frame is enabled by default. To disable this (and prevent click jacking attacks)
set the below:

.. code-block:: ini

    [webserver]
    x_frame_enabled = False

Disable Deployment Exposure Warning
---------------------------------------

Airflow warns when recent requests are made to ``/robots.txt``. To disable this warning set ``warn_deployment_exposure`` to
``False`` as below:

.. code-block:: ini

    [webserver]
    warn_deployment_exposure = False

Sensitive Variable fields
-------------------------

Variable values that are deemed "sensitive" based on the variable name will be masked in the UI automatically.
See :ref:`security:mask-sensitive-values` for more details.

.. _web-authentication:

Web Authentication
------------------

The webserver authentication is handled by the auth manager. For more information about webserver authentication, please refer to the auth manager documentation used by your environment.
By default Airflow uses the FAB auth manager, if you did not specify any other auth manager, please look at :doc:`apache-airflow-providers-fab:auth-manager/webserver-authentication`.

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

Worker and Triggerer Log Server SSL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. versionadded:: 3.0.0

The worker and triggerer log servers can be configured to use SSL/HTTPS for secure log transmission.
This encrypts the communication when the webserver fetches logs from workers and triggerers during task execution.

.. code-block:: ini

    [logging]
    worker_log_server_ssl_cert = <path to cert>
    worker_log_server_ssl_key = <path to key>
    worker_log_server_ssl_verify = True

For more information about configuring SSL for log servers, see :ref:`serving-worker-trigger-logs`.

Rate limiting
-------------

Airflow can be configured to limit the number of authentication requests in a given time window. We are using
`Flask-Limiter <https://flask-limiter.readthedocs.io/en/stable/>`_ to achieve that and by default Airflow
uses per-webserver default limit of 5 requests per 40 second fixed window. By default no common storage for
rate limits is used between the gunicorn processes you run so rate-limit is applied separately for each process,
so assuming random distribution of the requests by gunicorn with single webserver instance and default 4
gunicorn workers, the effective rate limit is 5 x 4 = 20 requests per 40 second window (more or less).
However you can configure the rate limit to be shared between the processes by using rate limit storage via
setting the ``RATELIMIT_*`` configuration settings in ``webserver_config.py``.
For example, to use Redis as a rate limit storage you can use the following configuration (you need
to set ``redis_host`` to your Redis instance)

.. code-block:: python

    RATELIMIT_STORAGE_URI = "redis://redis_host:6379/0"

You can also configure other rate limit settings in ``webserver_config.py`` - for more details, see the
`Flask Limiter rate limit configuration <https://flask-limiter.readthedocs.io/en/stable/configuration.html>`_.
