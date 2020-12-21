:mod:`airflow.providers.amazon.aws.hooks.secrets_manager`
=========================================================

.. py:module:: airflow.providers.amazon.aws.hooks.secrets_manager


Module Contents
---------------

.. py:class:: SecretsManagerHook(*args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon SecretsManager Service.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. see also::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   
   .. method:: get_secret(self, secret_name: str)

      Retrieve secret value from AWS Secrets Manager as a str or bytes
      reflecting format it stored in the AWS Secrets Manager

      :param secret_name: name of the secrets.
      :type secret_name: str
      :return: Union[str, bytes] with the information about the secrets
      :rtype: Union[str, bytes]



   
   .. method:: get_secret_as_dict(self, secret_name: str)

      Retrieve secret value from AWS Secrets Manager in a dict representation

      :param secret_name: name of the secrets.
      :type secret_name: str
      :return: dict with the information about the secrets
      :rtype: dict




