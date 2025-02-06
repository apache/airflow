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

:py:mod:`airflow.providers.amazon.aws.utils.connection_wrapper`
===============================================================

.. py:module:: airflow.providers.amazon.aws.utils.connection_wrapper


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.connection_wrapper.AwsConnectionWrapper




.. py:class:: AwsConnectionWrapper(context=None)


   Bases: :py:obj:`airflow.utils.log.logging_mixin.LoggingMixin`

   AWS Connection Wrapper class helper.

   Use for validate and resolve AWS Connection parameters.

   ``conn`` references an Airflow Connection object or AwsConnectionWrapper
       if it set to ``None`` than default values would use.

   The precedence rules for ``region_name``
       1. Explicit set (in Hook) ``region_name``.
       2. Airflow Connection Extra 'region_name'.

   The precedence rules for ``botocore_config``
       1. Explicit set (in Hook) ``botocore_config``.
       2. Construct from Airflow Connection Extra 'botocore_kwargs'.
       3. The wrapper's default value

   .. py:property:: extra_dejson

      Compatibility with `airflow.models.Connection.extra_dejson` property.


   .. py:property:: session_kwargs
      :type: dict[str, Any]

      Additional kwargs passed to boto3.session.Session.


   .. py:attribute:: conn
      :type: dataclasses.InitVar[airflow.models.connection.Connection | AwsConnectionWrapper | _ConnectionMetadata | None]



   .. py:attribute:: region_name
      :type: str | None



   .. py:attribute:: botocore_config
      :type: botocore.config.Config | None



   .. py:attribute:: verify
      :type: bool | str | None



   .. py:attribute:: conn_id
      :type: str | airflow.utils.types.ArgNotSet | None



   .. py:attribute:: conn_type
      :type: str | None



   .. py:attribute:: login
      :type: str | None



   .. py:attribute:: password
      :type: str | None



   .. py:attribute:: schema
      :type: str | None



   .. py:attribute:: extra_config
      :type: dict[str, Any]



   .. py:attribute:: aws_access_key_id
      :type: str | None



   .. py:attribute:: aws_secret_access_key
      :type: str | None



   .. py:attribute:: aws_session_token
      :type: str | None



   .. py:attribute:: profile_name
      :type: str | None



   .. py:attribute:: endpoint_url
      :type: str | None



   .. py:attribute:: role_arn
      :type: str | None



   .. py:attribute:: assume_role_method
      :type: str | None



   .. py:attribute:: assume_role_kwargs
      :type: dict[str, Any]



   .. py:attribute:: service_config
      :type: dict[str, dict[str, Any]]



   .. py:method:: conn_repr()


   .. py:method:: get_service_config(service_name)

      Get AWS Service related config dictionary.

      :param service_name: Name of botocore/boto3 service.


   .. py:method:: get_service_endpoint_url(service_name, *, sts_connection_assume = False, sts_test_connection = False)


   .. py:method:: __post_init__(conn)


   .. py:method:: from_connection_metadata(conn_id = None, login = None, password = None, extra = None)
      :classmethod:

      Create config from connection metadata.

      :param conn_id: Custom connection ID.
      :param login: AWS Access Key ID.
      :param password: AWS Secret Access Key.
      :param extra: Connection Extra metadata.


   .. py:method:: __bool__()
