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

.. _ssm_parameter_store_secrets:

AWS SSM Parameter Store Secrets Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable SSM parameter store, specify :py:class:`~airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend`
as the ``backend`` in  ``[secrets]`` section of ``airflow.cfg``.

Here is a sample configuration:

.. code-block:: ini

    [secrets]
    backend = airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend
    backend_kwargs = {"connections_prefix": "/airflow/connections", "variables_prefix": "/airflow/variables", "profile_name": "default"}

Storing and Retrieving Connections
""""""""""""""""""""""""""""""""""

If you have set ``connections_prefix`` as ``/airflow/connections``, then for a connection id of ``smtp_default``,
you would want to store your connection at ``/airflow/connections/smtp_default``.

Optionally you can supply a profile name to reference aws profile, e.g. defined in ``~/.aws/config``.

The value of the SSM parameter must be the :ref:`connection URI representation <generating_connection_uri>`
of the connection object.

Storing and Retrieving Variables
""""""""""""""""""""""""""""""""

If you have set ``variables_prefix`` as ``/airflow/variables``, then for an Variable key of ``hello``,
you would want to store your Variable at ``/airflow/variables/hello``.

Optionally you can supply a profile name to reference aws profile, e.g. defined in ``~/.aws/config``.
