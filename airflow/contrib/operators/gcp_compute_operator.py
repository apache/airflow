# -*- coding: utf-8 -*-
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

from airflow import AirflowException
from airflow.contrib.hooks.gcp_compute_hook import GceHook
from airflow.contrib.utils.gcp_field_validator import GcpBodyFieldValidator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GceBaseOperator(BaseOperator):
    """
    Abstract base operator for Google Compute Engine operators to inherit from.
    """
    @apply_defaults
    def __init__(self,
                 project_id,
                 zone,
                 resource_id,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args, **kwargs):
        self.project_id = project_id
        self.zone = zone
        self.full_location = 'projects/{}/zones/{}'.format(self.project_id,
                                                           self.zone)
        self.resource_id = resource_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()
        self._hook = GceHook(gcp_conn_id=self.gcp_conn_id, api_version=self.api_version)
        super(GceBaseOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if not self.project_id:
            raise AirflowException("The required parameter 'project_id' is missing")
        if not self.zone:
            raise AirflowException("The required parameter 'zone' is missing")
        if not self.resource_id:
            raise AirflowException("The required parameter 'resource_id' is missing")

    def execute(self, context):
        pass


class GceInstanceStartOperator(GceBaseOperator):
    """
    Start an instance in Google Compute Engine.

    :param project_id: Google Cloud Platform project where the Compute Engine
        instance exists.
    :type project_id: str
    :param zone: Google Cloud Platform zone where the instance exists.
    :type zone: str
    :param resource_id: Name of the Compute Engine instance resource.
    :type resource_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    template_fields = ('project_id', 'zone', 'resource_id', 'gcp_conn_id', 'api_version')

    @apply_defaults
    def __init__(self,
                 project_id,
                 zone,
                 resource_id,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args, **kwargs):
        super(GceInstanceStartOperator, self).__init__(
            project_id=project_id, zone=zone, resource_id=resource_id,
            gcp_conn_id=gcp_conn_id, api_version=api_version, *args, **kwargs)

    def execute(self, context):
        return self._hook.start_instance(self.project_id, self.zone, self.resource_id)


class GceInstanceStopOperator(GceBaseOperator):
    """
    Stop an instance in Google Compute Engine.

    :param project_id: Google Cloud Platform project where the Compute Engine
        instance exists.
    :type project_id: str
    :param zone: Google Cloud Platform zone where the instance exists.
    :type zone: str
    :param resource_id: Name of the Compute Engine instance resource.
    :type resource_id: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    template_fields = ('project_id', 'zone', 'resource_id', 'gcp_conn_id', 'api_version')

    @apply_defaults
    def __init__(self,
                 project_id,
                 zone,
                 resource_id,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args, **kwargs):
        super(GceInstanceStopOperator, self).__init__(
            project_id=project_id, zone=zone, resource_id=resource_id,
            gcp_conn_id=gcp_conn_id, api_version=api_version, *args, **kwargs)

    def execute(self, context):
        return self._hook.stop_instance(self.project_id, self.zone, self.resource_id)


SET_MACHINE_TYPE_VALIDATION_SPECIFICATION = [
    dict(name="machineType", regexp="^.+$"),
]


class GceSetMachineTypeOperator(GceBaseOperator):
    """
    Changes the machine type for a stopped instance to the machine type specified in
    the request.

    :param project_id: Google Cloud Platform project where the Compute Engine
        instance exists.
    :type project_id: str
    :param zone: Google Cloud Platform zone where the instance exists.
    :type zone: str
    :param resource_id: Name of the Compute Engine instance resource.
    :type resource_id: str
    :param body: Body required by the Compute Engine setMachineType API, as described in
        https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType#request-body
    :type body: dict
    :param gcp_conn_id: The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: API version used (e.g. v1).
    :type api_version: str
    """
    template_fields = ('project_id', 'zone', 'resource_id', 'gcp_conn_id', 'api_version')

    @apply_defaults
    def __init__(self,
                 project_id,
                 zone,
                 resource_id,
                 body,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 validate_body=True,
                 *args, **kwargs):
        self.body = body
        self._field_validator = None
        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                SET_MACHINE_TYPE_VALIDATION_SPECIFICATION, api_version=api_version)
        super(GceSetMachineTypeOperator, self).__init__(
            project_id=project_id, zone=zone, resource_id=resource_id,
            gcp_conn_id=gcp_conn_id, api_version=api_version, *args, **kwargs)

    def _validate_all_body_fields(self):
        if self._field_validator:
            self._field_validator.validate(self.body)

    def execute(self, context):
        self._validate_all_body_fields()
        return self._hook.set_machine_type(self.project_id, self.zone,
                                           self.resource_id, self.body)
