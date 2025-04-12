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

- Configure your reverse  proxy (e.g. nginx) to pass the url and http header as it for the Airflow webserver, without any rewrite, for example::

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

- Use ``--proxy-headers`` CLI flag to tell Uvicorn to respect these headers: ``airflow api-server --proxy-headers``

- If your proxy server is not on the same host (or in the same docker container) as Airflow, then you will need to
  set the ``FORWARDED_ALLOW_IPS`` environment variable so Uvicorn knows who to trust this header from. See
  `Uvicorn's docs <https://www.uvicorn.org/deployment/#proxies-and-forwarded-headers>`_. For the full options you can pass here.
  (Please note the ``--forwarded-allow-ips`` CLI option does not exist in Airflow.)

.. spelling::

  Uvicorn
