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

:py:mod:`airflow.providers.amazon.aws.hooks.lambda_function`
============================================================

.. py:module:: airflow.providers.amazon.aws.hooks.lambda_function

.. autoapi-nested-parse::

   This module contains AWS Lambda hook.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook




.. py:class:: LambdaHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS Lambda.

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("lambda") <Lambda.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:method:: invoke_lambda(*, function_name, invocation_type = None, log_type = None, client_context = None, payload = None, qualifier = None)

      Invoke Lambda Function.

      .. seealso::
          - :external+boto3:py:meth:`Lambda.Client.invoke`

      :param function_name: AWS Lambda Function Name
      :param invocation_type: AWS Lambda Invocation Type (RequestResponse, Event etc)
      :param log_type: Set to Tail to include the execution log in the response.
          Applies to synchronously invoked functions only.
      :param client_context: Up to 3,583 bytes of base64-encoded data about the invoking client
          to pass to the function in the context object.
      :param payload: The JSON that you want to provide to your Lambda function as input.
      :param qualifier: AWS Lambda Function Version or Alias Name


   .. py:method:: create_lambda(*, function_name, runtime = None, role, handler = None, code, description = None, timeout = None, memory_size = None, publish = None, vpc_config = None, package_type = None, dead_letter_config = None, environment = None, kms_key_arn = None, tracing_config = None, tags = None, layers = None, file_system_configs = None, image_config = None, code_signing_config_arn = None, architectures = None)

      Create a Lambda function.

      .. seealso::
          - :external+boto3:py:meth:`Lambda.Client.create_function`
          - `Configuring a Lambda function to access resources in a VPC             <https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html>`__

      :param function_name: AWS Lambda Function Name
      :param runtime: The identifier of the function's runtime.
          Runtime is required if the deployment package is a .zip file archive.
      :param role: The Amazon Resource Name (ARN) of the function's execution role.
      :param handler: The name of the method within your code that Lambda calls to run your function.
          Handler is required if the deployment package is a .zip file archive.
      :param code: The code for the function.
      :param description: A description of the function.
      :param timeout: The amount of time (in seconds) that Lambda
          allows a function to run before stopping it.
      :param memory_size: The amount of memory available to the function at runtime.
          Increasing the function memory also increases its CPU allocation.
      :param publish: Set to true to publish the first version of the function during creation.
      :param vpc_config: For network connectivity to Amazon Web Services resources in a VPC,
          specify a list of security groups and subnets in the VPC.
      :param package_type: The type of deployment package.
          Set to `Image` for container image and set to `Zip` for .zip file archive.
      :param dead_letter_config: A dead-letter queue configuration that specifies the queue or topic
          where Lambda sends asynchronous events when they fail processing.
      :param environment: Environment variables that are accessible from function code during execution.
      :param kms_key_arn: The ARN of the Key Management Service (KMS) key that's used to
          encrypt your function's environment variables.
          If it's not provided, Lambda uses a default service key.
      :param tracing_config: Set `Mode` to `Active` to sample and trace
          a subset of incoming requests with X-Ray.
      :param tags: A list of tags to apply to the function.
      :param layers: A list of function layers to add to the function's execution environment.
          Specify each layer by its ARN, including the version.
      :param file_system_configs: Connection settings for an Amazon EFS file system.
      :param image_config: Container image configuration values that override
          the values in the container image Dockerfile.
      :param code_signing_config_arn: To enable code signing for this function,
          specify the ARN of a code-signing configuration.
          A code-signing configuration includes a set of signing profiles,
          which define the trusted publishers for this function.
      :param architectures: The instruction set architecture that the function supports.


   .. py:method:: encode_log_result(log_result, *, keep_empty_lines = True)
      :staticmethod:

      Encode execution log from the response and return list of log records.

      Returns ``None`` on error, e.g. invalid base64-encoded string

      :param log_result: base64-encoded string which contain Lambda execution Log.
      :param keep_empty_lines: Whether or not keep empty lines.
