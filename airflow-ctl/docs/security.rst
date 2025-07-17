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

Security
========
Airflow CTL is leveraging Apache Airflow Public API security features and additional layers of security to ensure that your data is safe and secure.
Airflow CTL facilitates the seamless deployment of CLI and API features together, reducing redundancy and simplifying maintenance. Transitioning from direct database access to an API-driven model will enhance the CLI's capabilities and improve security.

- **Authentication**: Airflow CTL uses authentication to ensure that only authorized users can access the system. This is done using an API token. See more on https://airflow.apache.org/docs/apache-airflow/stable/security/api.html

- **Keyring**: Airflow CTL uses keyring to store the API token securely. This ensures that the token is not stored in plain text and is only accessible to authorized users.

Airflow CTL API Token has its own expiration time. The default is 1 hour. You can change it in the Airflow configuration file (airflow.cfg) by setting the ``jwt_cli_expiration_time`` parameter under the ``[api_auth]`` section. The value is in seconds. This will impact all users using ``airflowctl``.
