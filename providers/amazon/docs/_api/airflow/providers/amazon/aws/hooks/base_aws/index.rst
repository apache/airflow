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

:py:mod:`airflow.providers.amazon.aws.hooks.base_aws`
=====================================================

.. py:module:: airflow.providers.amazon.aws.hooks.base_aws

.. autoapi-nested-parse::

   This module contains Base AWS Hook.

   .. seealso::
       For more information on how to use this hook, take a look at the guide:
       :ref:`howto/connection:aws`



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.base_aws.BaseSessionFactory
   airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook
   airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook
   airflow.providers.amazon.aws.hooks.base_aws.BaseAsyncSessionFactory
   airflow.providers.amazon.aws.hooks.base_aws.AwsBaseAsyncHook



Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.base_aws.resolve_session_factory



Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.base_aws.BaseAwsConnection
   airflow.providers.amazon.aws.hooks.base_aws.SessionFactory


.. py:data:: BaseAwsConnection



.. py:class:: BaseSessionFactory(conn, region_name = None, config = None)


   Bases: :py:obj:`airflow.utils.log.logging_mixin.LoggingMixin`

   Base AWS Session Factory class.

   This handles synchronous and async boto session creation. It can handle most
   of the AWS supported authentication methods.

   User can also derive from this class to have full control of boto3 session
   creation or to support custom federation.

   .. note::
       Not all features implemented for synchronous sessions are available
       for async sessions.

   .. seealso::
       - :ref:`howto/connection:aws:session-factory`

   .. py:property:: extra_config
      :type: dict[str, Any]

      AWS Connection extra_config.


   .. py:property:: region_name
      :type: str | None

      AWS Region Name read-only property.


   .. py:property:: config
      :type: botocore.config.Config | None

      Configuration for botocore client read-only property.


   .. py:property:: role_arn
      :type: str | None

      Assume Role ARN from AWS Connection.


   .. py:method:: conn()

      Cached AWS Connection Wrapper.


   .. py:method:: basic_session()

      Cached property with basic boto3.session.Session.


   .. py:method:: get_async_session()


   .. py:method:: create_session(deferrable = False)

      Create boto3 or aiobotocore Session from connection config.



.. py:class:: AwsGenericHook(aws_conn_id = default_conn_name, verify = None, region_name = None, client_type = None, resource_type = None, config = None)


   Bases: :py:obj:`airflow.hooks.base.BaseHook`, :py:obj:`Generic`\ [\ :py:obj:`BaseAwsConnection`\ ]

   Generic class for interact with AWS.

   This class provide a thin wrapper around the boto3 Python library.

   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).
   :param verify: Whether or not to verify SSL certificates. See:
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
   :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
   :param client_type: Reference to :external:py:meth:`boto3.client service_name         <boto3.session.Session.client>`, e.g. 'emr', 'batch', 's3', etc.
       Mutually exclusive with ``resource_type``.
   :param resource_type: Reference to :external:py:meth:`boto3.resource service_name         <boto3.session.Session.resource>`, e.g. 's3', 'ec2', 'dynamodb', etc.
       Mutually exclusive with ``client_type``.
   :param config: Configuration for botocore client. See:
       https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html

   .. py:property:: service_name
      :type: str

      Extracted botocore/boto3 service name from hook parameters.


   .. py:property:: service_config
      :type: dict

      Config for hook-specific service from AWS Connection.


   .. py:property:: region_name
      :type: str | None

      AWS Region Name read-only property.


   .. py:property:: config
      :type: botocore.config.Config

      Configuration for botocore client read-only property.


   .. py:property:: verify
      :type: bool | str | None

      Verify or not SSL certificates boto3 client/resource read-only property.


   .. py:property:: async_conn

      Get an aiobotocore client to use for async operations.


   .. py:property:: conn_client_meta
      :type: botocore.client.ClientMeta

      Get botocore client metadata from Hook connection (cached).


   .. py:property:: conn_region_name
      :type: str

      Get actual AWS Region Name from Hook connection (cached).


   .. py:property:: conn_partition
      :type: str

      Get associated AWS Region Partition from Hook connection (cached).


   .. py:attribute:: conn_name_attr
      :value: 'aws_conn_id'



   .. py:attribute:: default_conn_name
      :value: 'aws_default'



   .. py:attribute:: conn_type
      :value: 'aws'



   .. py:attribute:: hook_name
      :value: 'Amazon Web Services'



   .. py:method:: conn_config()

      Get the Airflow Connection object and wrap it in helper (cached).


   .. py:method:: get_session(region_name = None, deferrable = False)

      Get the underlying boto3.session.Session(region_name=region_name).


   .. py:method:: get_client_type(region_name = None, config = None, deferrable = False)

      Get the underlying boto3 client using boto3 session.


   .. py:method:: get_resource_type(region_name = None, config = None)

      Get the underlying boto3 resource using boto3 session.


   .. py:method:: conn()

      Get the underlying boto3 client/resource (cached).

      :return: boto3.client or boto3.resource


   .. py:method:: get_conn()

      Get the underlying boto3 client/resource (cached).

      Implemented so that caching works as intended. It exists for compatibility
      with subclasses that rely on a super().get_conn() method.

      :return: boto3.client or boto3.resource


   .. py:method:: get_credentials(region_name = None)

      Get the underlying `botocore.Credentials` object.

      This contains the following authentication attributes: access_key, secret_key and token.
      By use this method also secret_key and token will mask in tasks logs.


   .. py:method:: expand_role(role, region_name = None)

      Get the Amazon Resource Name (ARN) for the role.

      If IAM role is already an IAM role ARN, the value is returned unchanged.

      :param role: IAM role name or ARN
      :param region_name: Optional region name to get credentials for
      :return: IAM role ARN


   .. py:method:: retry(should_retry)
      :staticmethod:

      Repeat requests in response to exceeding a temporary quote limit.


   .. py:method:: get_ui_field_behaviour()
      :staticmethod:

      Return custom UI field behaviour for AWS Connection.


   .. py:method:: test_connection()

      Test the AWS connection by call AWS STS (Security Token Service) GetCallerIdentity API.

      .. seealso::
          https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html


   .. py:method:: waiter_path()


   .. py:method:: get_waiter(waiter_name, parameters = None, deferrable = False, client=None)

      Get a waiter by name.

      First checks if there is a custom waiter with the provided waiter_name and
      uses that if it exists, otherwise it will check the service client for a
      waiter that matches the name and pass that through.

      If `deferrable` is True, the waiter will be an AIOWaiter, generated from the
      client that is passed as a parameter. If `deferrable` is True, `client` must be
      provided.

      :param waiter_name: The name of the waiter.  The name should exactly match the
          name of the key in the waiter model file (typically this is CamelCase).
      :param parameters: will scan the waiter config for the keys of that dict,
          and replace them with the corresponding value. If a custom waiter has
          such keys to be expanded, they need to be provided here.
      :param deferrable: If True, the waiter is going to be an async custom waiter.
          An async client must be provided in that case.
      :param client: The client to use for the waiter's operations


   .. py:method:: list_waiters()

      Return a list containing the names of all waiters for the service, official and custom.



.. py:class:: AwsBaseHook(aws_conn_id = default_conn_name, verify = None, region_name = None, client_type = None, resource_type = None, config = None)


   Bases: :py:obj:`AwsGenericHook`\ [\ :py:obj:`Union`\ [\ :py:obj:`boto3.client`\ , :py:obj:`boto3.resource`\ ]\ ]

   Base class for interact with AWS.

   This class provide a thin wrapper around the boto3 Python library.

   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).
   :param verify: Whether or not to verify SSL certificates. See:
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
   :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
   :param client_type: Reference to :external:py:meth:`boto3.client service_name         <boto3.session.Session.client>`, e.g. 'emr', 'batch', 's3', etc.
       Mutually exclusive with ``resource_type``.
   :param resource_type: Reference to :external:py:meth:`boto3.resource service_name         <boto3.session.Session.resource>`, e.g. 's3', 'ec2', 'dynamodb', etc.
       Mutually exclusive with ``client_type``.
   :param config: Configuration for botocore client. See:
       https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html


.. py:function:: resolve_session_factory()

   Resolve custom SessionFactory class.


.. py:data:: SessionFactory



.. py:class:: BaseAsyncSessionFactory(*args, **kwargs)


   Bases: :py:obj:`BaseSessionFactory`

   Base AWS Session Factory class to handle aiobotocore session creation.

   It currently, handles ENV, AWS secret key and STS client method ``assume_role``
   provided in Airflow connection

   .. py:method:: get_role_credentials()
      :async:

      Get the role_arn, method credentials from connection and get the role credentials.


   .. py:method:: create_session(deferrable = False)

      Create aiobotocore Session from connection and config.



.. py:class:: AwsBaseAsyncHook(*args, **kwargs)


   Bases: :py:obj:`AwsBaseHook`

   Interacts with AWS using aiobotocore asynchronously.

   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default botocore behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default botocore configuration would be used (and must be
       maintained on each worker node).
   :param verify: Whether to verify SSL certificates.
   :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
   :param client_type: boto3.client client_type. Eg 's3', 'emr' etc
   :param resource_type: boto3.resource resource_type. Eg 'dynamodb' etc
   :param config: Configuration for botocore client.

   .. py:method:: get_async_session()

      Get the underlying aiobotocore.session.AioSession(...).


   .. py:method:: get_client_async()
      :async:

      Get the underlying aiobotocore client using aiobotocore session.
