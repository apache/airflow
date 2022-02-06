#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class SSMParameterStoreHook(AwsBaseHook):
    """
    Interact with Amazon SSM Service.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. see also::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(client_type='ssm', *args, **kwargs)

    def get_parameter(self, parameter_name: str, with_decryption: bool):
        """
        Retrieve parameter value from AWS Systems Manager Parameter Store.
        Reflecting format it stored in the AWS Systems Manager Parameter Store
        :param parameter_name: name of the parameter
        :param with_decryption: Return decrypted values for secure string parameters
        :return: str with the value of the Systems Manager Parameter
        :rtype: str
        """
        ssm_client = self.get_conn()
        try:
            get_parameter_response = ssm_client.get_parameter(
                Name=parameter_name, WithDecryption=with_decryption
            )
            if 'Parameter' in get_parameter_response:
                self.log.info("Returning value of the parameter")
                return get_parameter_response['Parameter']['Value']
        except ssm_client.exceptions.ParameterNotFound:
            self.log.error('Parameter Does Not Exist')
            raise
        except Exception as general_error:
            self.log.error("Failed to list SSM Parameter, error: %s", general_error)
            raise

    def get_parameters_by_path(self, path: str, **parameter_kwargs):
        """
        Retrieve value of one or more parameters matching the path from AWS Systems Manager Parameter Store.
        :param path: path/hierarchy for the parameters
        :param parameter_kwargs:  Keyword args that define the configurations used to retrieve the parameter
        :return: dict with the value of the Systems Manager Parameter
        :rtype: dict
        """
        ssm_client = self.get_conn()
        try:
            get_parameter_by_path_response = ssm_client.get_parameters_by_path(
                Path=path,
                **parameter_kwargs,
            )
            if 'Parameters' in get_parameter_by_path_response:
                self.log.info("Returning dictionary with the list of parameters")
                return get_parameter_by_path_response
        except Exception as general_error:
            self.log.error("Failed to list SSM Parameter, error: %s", general_error)
            raise

    def put_parameter(self, parameter_name: str, value: str, param_type: str, **parameter_kwargs):
        """
        Add parameter to the AWS Systems Manager Parameter Store
        :param parameter_name: name of the parameter
        :param value: Value of the parameter
        :param param_type: type of parameter
        :param parameter_kwargs: Keyword args that define the configurations used to create the parameter

        :return: dict containing version of parameter and tier
            For details of the returned value see :py:meth:`botocore.client.SSM.put_parameter`
        :rtype: dict
        """
        ssm_client = self.get_conn()
        try:
            get_parameter_response = ssm_client.put_parameter(
                Name=parameter_name,
                Value=value,
                Type=param_type,
                **parameter_kwargs,
            )
            return get_parameter_response
        except Exception as general_error:
            self.log.error("Failed to create SSM Parameter Store, error: %s", general_error)
            raise
