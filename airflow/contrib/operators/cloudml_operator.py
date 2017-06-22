#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the 'License'); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging
from airflow import settings
from airflow.contrib.hooks.gcp_cloudml_hook import CloudMLHook
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults

logging.getLogger('GoogleCloudML').setLevel(settings.LOGGING_LEVEL)


class CloudMLVersionOperator(BaseOperator):
    """
    Operator for managing a Google Cloud ML version.

    :param model_name: The name of the Google Cloud ML model that the version
        belongs to.
    :type model_name: string

    :param project_name: The Google Cloud project name to which CloudML
        model belongs.
    :type project_name: string

    :param version: A dictionary containing the information about the version.
        If the `operation` is `create`, `version` should contain all the
        information about this version such as name, and deploymentUrl.
        If the `operation` is `get` or `delete`, the `version` parameter
        should contain the `name` of the version.
        If it is None, the only `operation` possible would be `list`.
    :type version: dict

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: string

    :param operation: The operation to perform. Available operations are:
        'create': Creates a new version in the model specified by `model_name`,
            in which case the `version` parameter should contain all the
            information to create that version
            (e.g. `name`, `deploymentUrl`).
        'get': Gets full information of a particular version in the model
            specified by `model_name`.
            The name of the version should be specified in the `version`
            parameter.

        'list': Lists all available versions of the model specified
            by `model_name`.

        'delete': Deletes the version specified in `version` parameter from the
            model specified by `model_name`).
            The name of the version should be specified in the `version`
            parameter.
     :type operation: string

    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: string
    """


    template_fields = [
        '_model_name',
        '_version',
    ]

    @apply_defaults
    def __init__(self,
                 model_name,
                 project_name,
                 version=None,
                 gcp_conn_id='google_cloud_default',
                 operation='create',
                 delegate_to=None,
                 *args,
                 **kwargs):

        super(CloudMLVersionOperator, self).__init__(*args, **kwargs)
        self._model_name = model_name
        self._version = version
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to
        self._project_name = project_name
        self._operation = operation

    def execute(self, context):
        hook = CloudMLHook(
            gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)

        if self._operation == 'create':
            assert self._version is not None
            return hook.create_version(self._project_name, self._model_name,
                                       self._version)
        elif self._operation == 'set_default':
            return hook.set_default_version(
                self._project_name, self._model_name,
                self._version['name'])
        elif self._operation == 'list':
            return hook.list_versions(self._project_name, self._model_name)
        elif self._operation == 'delete':
            return hook.delete_version(self._project_name, self._model_name,
                                       self._version['name'])
        else:
            raise ValueError('Unknown operation: {}'.format(self._operation))


class CloudMLModelOperator(BaseOperator):
    """
    Operator for managing a Google Cloud ML model.

    :param model: A dictionary containing the information about the model.
        If the `operation` is `create`, then the `model` parameter should
        contain all the information about this model such as `name`.

        If the `operation` is `get`, the `model` parameter
        should contain the `name` of the model.
    :type model: dict

    :param project_name: The Google Cloud project name to which CloudML
        model belongs.
    :type project_name: string

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: string

    :param operation: The operation to perform. Available operations are:
        'create': Creates a new model as provided by the `model` parameter.
        'get': Gets a particular model where the name is specified in `model`.

    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: string
    """

    template_fields = [
        '_model',
    ]

    @apply_defaults
    def __init__(self,
                 model,
                 project_name,
                 gcp_conn_id='google_cloud_default',
                 operation='create',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(CloudMLModelOperator, self).__init__(*args, **kwargs)
        self._model = model
        self._operation = operation
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to
        self._project_name = project_name

    def execute(self, context):
        hook = CloudMLHook(
            gcp_conn_id=self._gcp_conn_id, delegate_to=self._delegate_to)
        if self._operation == 'create':
            hook.create_model(self._project_name, self._model)
        elif self._operation == 'get':
            hook.get_model(self._project_name, self._model['name'])
        else:
            raise ValueError('Unknown operation: {}'.format(self._operation))
