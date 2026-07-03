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


Configuration Reference
.......................

This page contains the list of all the available Airflow configurations that you
can set in ``airflow.cfg`` file or using environment variables.

Different Airflow components may require different configuration parameters, and for
improved security, you should restrict sensitive configuration to only the components that
need it. Some configuration values must be shared across specific components to work
correctly — for example, the JWT signing key (``[api_auth] jwt_secret`` or
``[api_auth] jwt_private_key_path``) must be consistent across all components that generate
or validate JWT tokens (Scheduler, API Server). However, other sensitive parameters such as
database connection strings or Fernet keys should only be provided to components that need them.

For security-sensitive deployments, pass configuration values via environment variables
scoped to individual components rather than sharing a single configuration file across all
components. See :doc:`/security/security_model` for details on which configuration
parameters should be restricted to which components.

Make sure that time on ALL the machines that you run Airflow components on is synchronized
(for example using ntpd) otherwise you might get "forbidden" errors when the logs are
accessed or API calls are made.

.. note::
    For more information see :doc:`/howto/set-config`.


Provider-specific configuration options
---------------------------------------

Some of the providers have their own configuration options, you will find details of their configuration
in the provider's documentation.

You can find all the provider configuration in
:doc:`configurations specific to providers <apache-airflow-providers:core-extensions/configurations>`

Airflow configuration options
-----------------------------


.. include:: /../../devel-common/src/sphinx_exts/includes/sections-and-options.rst
