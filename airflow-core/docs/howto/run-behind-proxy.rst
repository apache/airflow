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



Running Airflow behind a reverse proxy
======================================

Airflow can be set up behind a reverse proxy, with the ability to set its endpoint with great
flexibility.

For example, you can configure your reverse proxy to get:

::

    https://lab.mycompany.com/myorg/airflow/

To do so, you need to set the following setting in your ``airflow.cfg``::

    base_url = http://my_host/myorg/airflow

- Configure your reverse proxy (e.g. nginx) to pass the url and http header as-is to the Airflow webserver, without any rewrite, for example::

      server {
        listen 80;
        server_name lab.mycompany.com;

        location /myorg/airflow/ {
            proxy_pass http://localhost:8080;
            proxy_set_header Host $http_host;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            proxy_redirect off;
            proxy_http_version 1.1;
        }
      }

- Some parts of the UI are rendered inside iframes (Auth managers security links for instance), you need to make sure that you are not setting a restricted CSP for iframe rendering
  such as ``frame-ancestors 'none'``. You can set the CSP header in your reverse proxy configuration, for example::

      add_header Content-Security-Policy "frame-ancestors 'self';";

- The API server runs on one of two application servers, selected with the ``[api] server_type``
  option: `Uvicorn <https://www.uvicorn.org>`_, the default, or `Gunicorn <https://gunicorn.org>`_
  with Uvicorn workers. Both read the settings below.

- Use ``--proxy-headers`` CLI flag to tell the application server to respect these headers: ``airflow api-server --proxy-headers``

- If your proxy server is not on the same host (or in the same docker container) as Airflow, then you will need to
  set the ``FORWARDED_ALLOW_IPS`` environment variable so the application server knows who to trust this header from. See
  `Uvicorn's docs <https://www.uvicorn.org/deployment/#proxies-and-forwarded-headers>`_. For the full options you can pass here.
  (Please note the ``--forwarded-allow-ips`` CLI option does not exist in Airflow.)

- Please make sure your proxy does not enforce http-only status on the Set-Cookie headers.
  Airflow frontend needs to access the cookies through javascript, and a http-only flag would disturb this functionality.

Running multiple Airflow instances on the same host
---------------------------------------------------

If you serve several Airflow instances from the same host name (for example ``https://lab.mycompany.com``,
or ``localhost`` on different ports), give each instance its own path in ``[api] base_url``. Browsers scope
cookies by host name and path, not by port, so instances that share the same ``base_url`` path also share
the authentication cookie: logging in to one instance logs you out of the others.

Airflow scopes its authentication cookie to the ``base_url`` path since Airflow 3.1.8. With the FAB,
Keycloak or AWS auth manager, this also requires ``apache-airflow-providers-fab`` 3.5.0,
``apache-airflow-providers-keycloak`` 0.6.0 or ``apache-airflow-providers-amazon`` 9.23.0 or later.
For example::

    # instance serving production
    base_url = https://lab.mycompany.com/prod

    # instance serving development
    base_url = https://lab.mycompany.com/dev

Paths that share a prefix, such as ``/airflow`` and ``/airflow_dev``, are fine: the cookie path always
ends with ``/``, so the cookie of one instance is not sent to the other.

.. warning::

    The ``base_url`` path of one instance must not be a parent path of another's, such as ``/prod`` and
    ``/prod/dev``, or ``/`` and any other path. The browser sends the parent instance's cookie with every
    request under its path, so the other instance reads the parent instance's token instead of its own and
    cannot keep you logged in.

Alternatively, serve each instance from its own host name, for example ``prod.lab.mycompany.com`` and
``dev.lab.mycompany.com``.

Helm Chart Configuration
------------------------

When deploying Airflow using the Helm chart behind a reverse proxy (e.g., nginx ingress), you need to configure the API server to respect proxy headers.

Configure the API server arguments to include the ``--proxy-headers`` flag::

    apiServer:
      args: ["bash", "-c", "exec airflow api-server --proxy-headers"]

If your proxy server is not on the same host as Airflow, set the ``FORWARDED_ALLOW_IPS`` environment variable::

    apiServer:
      args: ["bash", "-c", "exec airflow api-server --proxy-headers"]
      env:
        - name: FORWARDED_ALLOW_IPS
          value: "*"  # Use "*" for trusted environments, or specify proxy IP ranges for production

Additionally, configure your ingress annotations to pass the necessary headers. For nginx ingress, add these annotations::

    ingress:
      apiServer:
        enabled: true
        annotations:
          nginx.ingress.kubernetes.io/proxy-http-version: "1.1"
          nginx.ingress.kubernetes.io/proxy-redirect-off: "true"
          nginx.ingress.kubernetes.io/configuration-snippet: |
            proxy_set_header Host $http_host;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
        hosts:
          - name: airflow.example.com
            tls:
              enabled: true
              secretName: airflow-tls

Make sure to also set the ``base_url`` in your Airflow configuration::

    config:
      api:
        base_url: https://airflow.example.com

.. spelling::

  Uvicorn
