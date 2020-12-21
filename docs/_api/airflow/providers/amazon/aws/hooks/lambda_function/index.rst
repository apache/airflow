:mod:`airflow.providers.amazon.aws.hooks.lambda_function`
=========================================================

.. py:module:: airflow.providers.amazon.aws.hooks.lambda_function

.. autoapi-nested-parse::

   This module contains AWS Lambda hook



Module Contents
---------------

.. py:class:: AwsLambdaHook(function_name: str, log_type: str = 'None', qualifier: str = '$LATEST', invocation_type: str = 'RequestResponse', *args, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS Lambda

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   :param function_name: AWS Lambda Function Name
   :type function_name: str
   :param log_type: Tail Invocation Request
   :type log_type: str
   :param qualifier: AWS Lambda Function Version or Alias Name
   :type qualifier: str
   :param invocation_type: AWS Lambda Invocation Type (RequestResponse, Event etc)
   :type invocation_type: str

   
   .. method:: invoke_lambda(self, payload: str)

      Invoke Lambda Function




