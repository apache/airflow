:mod:`airflow.providers.amazon.aws.hooks.base_aws`
==================================================

.. py:module:: airflow.providers.amazon.aws.hooks.base_aws

.. autoapi-nested-parse::

   This module contains Base AWS Hook.

   .. seealso::
       For more information on how to use this hook, take a look at the guide:
       :ref:`howto/connection:AWSHook`



Module Contents
---------------

.. py:class:: _SessionFactory(conn: Connection, region_name: Optional[str], config: Config)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   
   .. method:: create_session(self)

      Create AWS session.



   
   .. method:: _create_basic_session(self, session_kwargs: Dict[str, Any])



   
   .. method:: _impersonate_to_role(self, role_arn: str, session: boto3.session.Session, session_kwargs: Dict[str, Any])



   
   .. method:: _read_role_arn_from_extra_config(self)



   
   .. method:: _read_credentials_from_connection(self)



   
   .. method:: _assume_role(self, sts_client: boto3.client, role_arn: str, assume_role_kwargs: Dict[str, Any])



   
   .. method:: _assume_role_with_saml(self, sts_client: boto3.client, role_arn: str, assume_role_kwargs: Dict[str, Any])



   
   .. method:: _fetch_saml_assertion_using_http_spegno_auth(self, saml_config: Dict[str, Any])



   
   .. method:: _assume_role_with_web_identity(self, role_arn, assume_role_kwargs, base_session)



   
   .. method:: _get_google_identity_token_loader(self)




.. py:class:: AwsBaseHook(aws_conn_id: Optional[str] = 'aws_default', verify: Union[bool, str, None] = None, region_name: Optional[str] = None, client_type: Optional[str] = None, resource_type: Optional[str] = None, config: Optional[Config] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Interact with AWS.
   This class is a thin wrapper around the boto3 python library.

   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).
   :type aws_conn_id: str
   :param verify: Whether or not to verify SSL certificates.
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
   :type verify: Union[bool, str, None]
   :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
   :type region_name: Optional[str]
   :param client_type: boto3.client client_type. Eg 's3', 'emr' etc
   :type client_type: Optional[str]
   :param resource_type: boto3.resource resource_type. Eg 'dynamodb' etc
   :type resource_type: Optional[str]
   :param config: Configuration for botocore client.
       (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)
   :type config: Optional[botocore.client.Config]

   
   .. method:: _get_credentials(self, region_name: Optional[str])



   
   .. method:: get_client_type(self, client_type: str, region_name: Optional[str] = None, config: Optional[Config] = None)

      Get the underlying boto3 client using boto3 session



   
   .. method:: get_resource_type(self, resource_type: str, region_name: Optional[str] = None, config: Optional[Config] = None)

      Get the underlying boto3 resource using boto3 session



   
   .. method:: conn(self)

      Get the underlying boto3 client/resource (cached)

      :return: boto3.client or boto3.resource
      :rtype: Union[boto3.client, boto3.resource]



   
   .. method:: get_conn(self)

      Get the underlying boto3 client/resource (cached)

      Implemented so that caching works as intended. It exists for compatibility
      with subclasses that rely on a super().get_conn() method.

      :return: boto3.client or boto3.resource
      :rtype: Union[boto3.client, boto3.resource]



   
   .. method:: get_session(self, region_name: Optional[str] = None)

      Get the underlying boto3.session.



   
   .. method:: get_credentials(self, region_name: Optional[str] = None)

      Get the underlying `botocore.Credentials` object.

      This contains the following authentication attributes: access_key, secret_key and token.



   
   .. method:: expand_role(self, role: str)

      If the IAM role is a role name, get the Amazon Resource Name (ARN) for the role.
      If IAM role is already an IAM role ARN, no change is made.

      :param role: IAM role name or ARN
      :return: IAM role ARN




.. function:: _parse_s3_config(config_file_name: str, config_format: Optional[str] = 'boto', profile: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]
   Parses a config file for s3 credentials. Can currently
   parse boto, s3cmd.conf and AWS SDK config formats

   :param config_file_name: path to the config file
   :type config_file_name: str
   :param config_format: config type. One of "boto", "s3cmd" or "aws".
       Defaults to "boto"
   :type config_format: str
   :param profile: profile name in AWS type config file
   :type profile: str


