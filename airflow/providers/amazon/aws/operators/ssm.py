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

from typing import TYPE_CHECKING, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.ssm_parameter_store import SSMParameterStoreHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SSMGetParameterOperator(BaseOperator):
    """
    Get value of a single parameter from AWS Systems manager parameter store by specifying the parameter name
    :param parameter_name: Parameter Name
    :param with_decryption: Return decrypted values for secure string parameter type
    :param aws_conn_id: The AWS connection ID to use
    """

    template_fields: Sequence[str] = ('parameter_name', 'with_decryption')
    ui_color = '#ffd700'

    def __init__(
        self,
        *,
        parameter_name: str,
        with_decryption: bool,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.parameter_name = parameter_name
        self.with_decryption = with_decryption
        self.aws_conn_id = aws_conn_id

    def execute(self, context: 'Context'):
        hook = SSMParameterStoreHook(aws_conn_id=self.aws_conn_id)
        self.log.info('Getting the value of SSM parameter: %s)', self.parameter_name)
        parameter_value = hook.get_parameter(
            parameter_name=self.parameter_name, with_decryption=self.with_decryption
        )
        return parameter_value


class SSMGetParameterByPathOperator(BaseOperator):
    """
    Retrieve information about one or more parameters in a specific hierarchy based on path
    :param path: hierarchy for the parameter. Should start with '/'
    :param parameter_kwargs: Dictionary containing additional key arguments to pass
    :param aws_conn_id: The AWS connection ID to use
    """

    template_fields: Sequence[str] = ('path', 'parameter_kwargs')
    ui_color = '#ffd700'

    def __init__(
        self,
        *,
        path: str,
        aws_conn_id: str = 'aws_default',
        parameter_kwargs: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.path = path
        self.parameter_kwargs = parameter_kwargs or {}
        self.aws_conn_id = aws_conn_id

    def execute(self, context: 'Context'):
        hook = SSMParameterStoreHook(aws_conn_id=self.aws_conn_id)
        self.log.info('Retrieving the parameters hierarchy : %s and values)', self.path)
        parameter_value = hook.get_parameters_by_path(path=self.path, **self.parameter_kwargs)
        return parameter_value


class SSMCreateParameterOperator(BaseOperator):
    """
    Add a parameter to the AWS Systems manager Parameter Store
     :param parameter_name: Name of the parameter
     :param value: Value of the parameter
     :param param_type: type of the parameter.Can be 'String'|'StringList'|'SecureString'
     :param parameter_kwargs: Dictionary containing additional key arguments to pass
     :param aws_conn_id: The AWS connection ID to use
    """

    template_fields: Sequence[str] = ('parameter_name', 'value', 'param_type', 'parameter_kwargs')
    ui_color = '#ffd700'

    def __init__(
        self,
        *,
        parameter_name: str,
        value: str,
        param_type: str,
        aws_conn_id: str = 'aws_default',
        parameter_kwargs: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.parameter_name = parameter_name
        self.value = value
        self.param_type = param_type
        self.aws_conn_id = aws_conn_id
        self.parameter_kwargs = parameter_kwargs or {}

    def execute(self, context: 'Context'):
        hook = SSMParameterStoreHook(aws_conn_id=self.aws_conn_id)
        hook.put_parameter(
            parameter_name=self.parameter_name,
            value=self.value,
            param_type=self.param_type,
            **self.parameter_kwargs,
        )
        self.log.info('New SSM parameter: %s is created)', self.parameter_name)
