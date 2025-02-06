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

:py:mod:`airflow.providers.amazon.aws.secrets.systems_manager`
==============================================================

.. py:module:: airflow.providers.amazon.aws.secrets.systems_manager

.. autoapi-nested-parse::

   Objects relating to sourcing connections from AWS SSM Parameter Store.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend




.. py:class:: SystemsManagerParameterStoreBackend(connections_prefix = '/airflow/connections', connections_lookup_pattern = None, variables_prefix = '/airflow/variables', variables_lookup_pattern = None, config_prefix = '/airflow/config', config_lookup_pattern = None, **kwargs)


   Bases: :py:obj:`airflow.secrets.BaseSecretsBackend`, :py:obj:`airflow.utils.log.logging_mixin.LoggingMixin`

   Retrieves Connection or Variables from AWS SSM Parameter Store.

   Configurable via ``airflow.cfg`` like so:

   .. code-block:: ini

       [secrets]
       backend = airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend
       backend_kwargs = {"connections_prefix": "/airflow/connections", "profile_name": null}

   For example, if ssm path is ``/airflow/connections/smtp_default``, this would be accessible
   if you provide ``{"connections_prefix": "/airflow/connections"}`` and request conn_id ``smtp_default``.
   And if ssm path is ``/airflow/variables/hello``, this would be accessible
   if you provide ``{"variables_prefix": "/airflow/variables"}`` and variable key ``hello``.

   :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
       If set to None (null), requests for connections will not be sent to AWS SSM Parameter Store.
   :param connections_lookup_pattern: Specifies a pattern the connection ID needs to match to be looked up in
       AWS Parameter Store. Applies only if `connections_prefix` is not None.
       If set to None (null value in the configuration), all connections will be looked up first in
       AWS Parameter Store.
   :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
       If set to None (null), requests for variables will not be sent to AWS SSM Parameter Store.
   :param variables_lookup_pattern: Specifies a pattern the variable key needs to match to be looked up in
       AWS Parameter Store. Applies only if `variables_prefix` is not None.
       If set to None (null value in the configuration), all variables will be looked up first in
       AWS Parameter Store.
   :param config_prefix: Specifies the prefix of the secret to read to get Variables.
       If set to None (null), requests for configurations will not be sent to AWS SSM Parameter Store.
   :param config_lookup_pattern: Specifies a pattern the config key needs to match to be looked up in
       AWS Parameter Store. Applies only if `config_prefix` is not None.
       If set to None (null value in the configuration), all config keys will be looked up first in
       AWS Parameter Store.

   You can also pass additional keyword arguments listed in AWS Connection Extra config
   to this class, and they would be used for establish connection and passed on to Boto3 client.

   .. code-block:: ini

       [secrets]
       backend = airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend
       backend_kwargs = {"connections_prefix": "airflow/connections", "region_name": "eu-west-1"}

   .. seealso::
       :ref:`howto/connection:aws:configuring-the-connection`


   .. py:method:: client()

      Create a SSM client.


   .. py:method:: get_conn_value(conn_id)

      Get param value.

      :param conn_id: connection id


   .. py:method:: get_variable(key)

      Get Airflow Variable.

      :param key: Variable Key
      :return: Variable Value


   .. py:method:: get_config(key)

      Get Airflow Configuration.

      :param key: Configuration Option Key
      :return: Configuration Option Value
