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

:py:mod:`airflow.providers.amazon.aws.secrets.secrets_manager`
==============================================================

.. py:module:: airflow.providers.amazon.aws.secrets.secrets_manager

.. autoapi-nested-parse::

   Objects relating to sourcing secrets from AWS Secrets Manager.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend




.. py:class:: SecretsManagerBackend(connections_prefix = 'airflow/connections', connections_lookup_pattern = None, variables_prefix = 'airflow/variables', variables_lookup_pattern = None, config_prefix = 'airflow/config', config_lookup_pattern = None, sep = '/', extra_conn_words = None, **kwargs)


   Bases: :py:obj:`airflow.secrets.BaseSecretsBackend`, :py:obj:`airflow.utils.log.logging_mixin.LoggingMixin`

   Retrieves Connection or Variables from AWS Secrets Manager.

   Configurable via ``airflow.cfg`` like so:

   .. code-block:: ini

       [secrets]
       backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
       backend_kwargs = {"connections_prefix": "airflow/connections"}

   For example, when ``{"connections_prefix": "airflow/connections"}`` is set, if a secret is defined with
   the path ``airflow/connections/smtp_default``, the connection with conn_id ``smtp_default`` would be
   accessible.

   When ``{"variables_prefix": "airflow/variables"}`` is set, if a secret is defined with
   the path ``airflow/variables/hello``, the variable with the name ``hello`` would be accessible.

   When ``{"config_prefix": "airflow/config"}`` set, if a secret is defined with
   the path ``airflow/config/sql_alchemy_conn``, the config with they ``sql_alchemy_conn`` would be
   accessible.

   You can also pass additional keyword arguments listed in AWS Connection Extra config
   to this class, and they would be used for establishing a connection and passed on to Boto3 client.

   .. code-block:: ini

       [secrets]
       backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
       backend_kwargs = {"connections_prefix": "airflow/connections", "region_name": "eu-west-1"}

   .. seealso::
       :ref:`howto/connection:aws:configuring-the-connection`

   There are two ways of storing secrets in Secret Manager for using them with this operator:
   storing them as a conn URI in one field, or taking advantage of native approach of Secrets Manager
   and storing them in multiple fields. There are certain words that will be searched in the name
   of fields for trying to retrieve a connection part. Those words are:

   .. code-block:: python

       possible_words_for_conn_fields = {
           "login": ["login", "user", "username", "user_name"],
           "password": ["password", "pass", "key"],
           "host": ["host", "remote_host", "server"],
           "port": ["port"],
           "schema": ["database", "schema"],
           "conn_type": ["conn_type", "conn_id", "connection_type", "engine"],
       }

   However, these lists can be extended using the configuration parameter ``extra_conn_words``. Also,
   you can have a field named extra for extra parameters for the conn. Please note that this extra field
   must be a valid JSON.

   :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
       If set to None (null value in the configuration), requests for connections will not be
       sent to AWS Secrets Manager. If you don't want a connections_prefix, set it as an empty string
   :param connections_lookup_pattern: Specifies a pattern the connection ID needs to match to be looked up in
       AWS Secrets Manager. Applies only if `connections_prefix` is not None.
       If set to None (null value in the configuration), all connections will be looked up first in
       AWS Secrets Manager.
   :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
       If set to None (null value in the configuration), requests for variables will not be sent to
       AWS Secrets Manager. If you don't want a variables_prefix, set it as an empty string
   :param variables_lookup_pattern: Specifies a pattern the variable key needs to match to be looked up in
       AWS Secrets Manager. Applies only if `variables_prefix` is not None.
       If set to None (null value in the configuration), all variables will be looked up first in
       AWS Secrets Manager.
   :param config_prefix: Specifies the prefix of the secret to read to get Configurations.
       If set to None (null value in the configuration), requests for configurations will not be sent to
       AWS Secrets Manager. If you don't want a config_prefix, set it as an empty string
   :param config_lookup_pattern: Specifies a pattern the config key needs to match to be looked up in
       AWS Secrets Manager. Applies only if `config_prefix` is not None.
       If set to None (null value in the configuration), all config keys will be looked up first in
       AWS Secrets Manager.
   :param sep: separator used to concatenate secret_prefix and secret_id. Default: "/"
   :param extra_conn_words: for using just when you set full_url_mode as false and store
       the secrets in different fields of secrets manager. You can add more words for each connection
       part beyond the default ones. The extra words to be searched should be passed as a dict of lists,
       each list corresponding to a connection part. The optional keys of the dict must be: user,
       password, host, schema, conn_type.

   .. py:method:: client()

      Create a Secrets Manager client.


   .. py:method:: get_conn_value(conn_id)

      Get serialized representation of Connection.

      :param conn_id: connection id


   .. py:method:: get_variable(key)

      Get Airflow Variable.

      :param key: Variable Key
      :return: Variable Value


   .. py:method:: get_config(key)

      Get Airflow Configuration.

      :param key: Configuration Option Key
      :return: Configuration Option Value
