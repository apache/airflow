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
from copy import deepcopy

from googleapiclient.errors import HttpError

from airflow import AirflowException
from airflow.contrib.hooks.gcp_compute_hook import GceHook
from airflow.contrib.utils.gcp_field_sanitizer import GcpBodyFieldSanitizer
from airflow.contrib.utils.gcp_field_validator import GcpBodyFieldValidator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from json_merge_patch import merge


class GceBaseOperator(BaseOperator):
    """
    Abstract base operator for Google Compute Engine operators to inherit from.
    """

    @apply_defaults
    def __init__(self,
                 zone,
                 resource_id,
                 project_id=None,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 *args, **kwargs):
        self.project_id = project_id
        self.zone = zone
        self.resource_id = resource_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self._validate_inputs()
        self._hook = GceHook(gcp_conn_id=self.gcp_conn_id, api_version=self.api_version)
        super(GceBaseOperator, self).__init__(*args, **kwargs)

    def _validate_inputs(self):
        if self.project_id == '':
            raise AirflowException("The required parameter 'project_id' is missing")
        if not self.zone:
            raise AirflowException("The required parameter 'zone' is missing")
        if not self.resource_id:
            raise AirflowException("The required parameter 'resource_id' is missing")

    def execute(self, context):
        pass


class GceInstanceStartOperator(GceBaseOperator):
    """
    Starts an instance in Google Compute Engine.

    :param zone: Google Cloud Platform zone where the instance exists.
    :type zone: str
    :param resource_id: Name of the Compute Engine instance resource.
    :type resource_id: str
    :param project_id: Optional, Google Cloud Platform Project ID where the Compute
        Engine Instance exists.
    :type project_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :type api_version: str
    :param validate_body: Optional, If set to False, body validation is not performed.
        Defaults to False.
    """
    # [START gce_instance_start_template_fields]
    template_fields = ('project_id', 'zone', 'resource_id', 'gcp_conn_id', 'api_version')
    # [END gce_instance_start_template_fields]

    @apply_defaults
    def __init__(self,
                 zone,
                 resource_id,
                 project_id=None,
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
    Stops an instance in Google Compute Engine.

    :param zone: Google Cloud Platform zone where the instance exists.
    :type zone: str
    :param resource_id: Name of the Compute Engine instance resource.
    :type resource_id: str
    :param project_id: Optional, Google Cloud Platform Project ID where the Compute
        Engine Instance exists.
    :type project_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :type api_version: str
    :param validate_body: Optional, If set to False, body validation is not performed.
        Defaults to False.
    """
    # [START gce_instance_stop_template_fields]
    template_fields = ('project_id', 'zone', 'resource_id', 'gcp_conn_id', 'api_version')
    # [END gce_instance_stop_template_fields]

    @apply_defaults
    def __init__(self,
                 zone,
                 resource_id,
                 project_id=None,
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

    :param zone: Google Cloud Platform zone where the instance exists.
    :type zone: str
    :param resource_id: Name of the Compute Engine instance resource.
    :type resource_id: str
    :param body: Body required by the Compute Engine setMachineType API, as described in
        https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType#request-body
    :type body: dict
    :param project_id: Optional, Google Cloud Platform Project ID where the Compute
        Engine Instance exists.
    :type project_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :type api_version: str
    :param validate_body: Optional, If set to False, body validation is not performed.
        Defaults to False.
    :type validate_body: bool
    """
    # [START gce_instance_set_machine_type_template_fields]
    template_fields = ('project_id', 'zone', 'resource_id', 'gcp_conn_id', 'api_version')
    # [END gce_instance_set_machine_type_template_fields]

    @apply_defaults
    def __init__(self,
                 zone,
                 resource_id,
                 body,
                 project_id=None,
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


GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION = [
    dict(name="name", regexp="^.+$"),
    dict(name="description", optional=True),
    dict(name="properties", type='dict', optional=True, fields=[
        dict(name="description", optional=True),
        dict(name="tags", optional=True, fields=[
            dict(name="items", optional=True)
        ]),
        dict(name="machineType", optional=True),
        dict(name="canIpForward", optional=True),
        dict(name="networkInterfaces", optional=True),  # not validating deeper
        dict(name="disks", optional=True),  # not validating the array deeper
        dict(name="metadata", optional=True, fields=[
            dict(name="fingerprint", optional=True),
            dict(name="items", optional=True),
            dict(name="kind", optional=True),
        ]),
        dict(name="serviceAccounts", optional=True),  # not validating deeper
        dict(name="scheduling", optional=True, fields=[
            dict(name="onHostMaintenance", optional=True),
            dict(name="automaticRestart", optional=True),
            dict(name="preemptible", optional=True),
            dict(name="nodeAffinitites", optional=True),  # not validating deeper
        ]),
        dict(name="labels", optional=True),
        dict(name="guestAccelerators", optional=True),  # not validating deeper
        dict(name="minCpuPlatform", optional=True),
    ]),
]

GCE_INSTANCE_TEMPLATE_FIELDS_TO_SANITIZE = [
    "kind",
    "id",
    "name",
    "creationTimestamp",
    "properties.disks.sha256",
    "properties.disks.kind",
    "properties.disks.sourceImageEncryptionKey.sha256",
    "properties.disks.index",
    "properties.disks.licenses",
    "properties.networkInterfaces.kind",
    "properties.networkInterfaces.accessConfigs.kind",
    "properties.networkInterfaces.name",
    "properties.metadata.kind",
    "selfLink"
]


class GceInstanceTemplateCopyOperator(GceBaseOperator):
    """
    Copies the instance template, applying specified changes.

    :param resource_id: Name of the Instance Template
    :type resource_id: str
    :param body_patch: Patch to the body of instanceTemplates object following rfc7386
        PATCH semantics. The body_patch content follows
        https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates
        Name field is required as we need to rename the template,
        all the other fields are optional. It is important to follow PATCH semantics
        - arrays are replaced fully, so if you need to update an array you should
        provide the whole target array as patch element.
    :type body_patch: dict
    :param project_id: Optional, Google Cloud Platform Project ID where the Compute
        Engine Instance exists.
    :type project_id: str
    :param request_id: Optional, unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new instance template again).
        It should be in UUID format as defined in RFC 4122.
    :type request_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :type api_version: str
    :param validate_body: Optional, If set to False, body validation is not performed.
        Defaults to False.
    :type validate_body: bool
    """
    # [START gce_instance_template_copy_operator_template_fields]
    template_fields = ('project_id', 'resource_id', 'request_id',
                       'gcp_conn_id', 'api_version')
    # [END gce_instance_template_copy_operator_template_fields]

    @apply_defaults
    def __init__(self,
                 resource_id,
                 body_patch,
                 project_id=None,
                 request_id=None,
                 gcp_conn_id='google_cloud_default',
                 api_version='v1',
                 validate_body=True,
                 *args, **kwargs):
        self.body_patch = body_patch
        self.request_id = request_id
        self._field_validator = None
        if 'name' not in self.body_patch:
            raise AirflowException("The body '{}' should contain at least "
                                   "name for the new operator in the 'name' field".
                                   format(body_patch))
        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION, api_version=api_version)
        self._field_sanitizer = GcpBodyFieldSanitizer(
            GCE_INSTANCE_TEMPLATE_FIELDS_TO_SANITIZE)
        super(GceInstanceTemplateCopyOperator, self).__init__(
            project_id=project_id, zone='global', resource_id=resource_id,
            gcp_conn_id=gcp_conn_id, api_version=api_version, *args, **kwargs)

    def _validate_all_body_fields(self):
        if self._field_validator:
            self._field_validator.validate(self.body_patch)

    def execute(self, context):
        self._validate_all_body_fields()
        try:
            # Idempotence check (sort of) - we want to check if the new template
            # is already created and if is, then we assume it was created by previous run
            # of CopyTemplate operator - we do not check if content of the template
            # is as expected. Templates are immutable so we cannot update it anyway
            # and deleting/recreating is not worth the hassle especially
            # that we cannot delete template if it is already used in some Instance
            # Group Manager. We assume success if the template is simply present
            existing_template = self._hook.get_instance_template(
                project_id=self.project_id, resource_id=self.body_patch['name'])
            self.log.info("The {} template already existed. It was likely "
                          "created by previous run of the operator. Assuming success.")
            return existing_template
        except HttpError as e:
            # We actually expect to get 404 / Not Found here as the template should
            # not yet exist
            if not e.resp.status == 404:
                raise e
        old_body = self._hook.get_instance_template(project_id=self.project_id,
                                                    resource_id=self.resource_id)
        new_body = deepcopy(old_body)
        self._field_sanitizer.sanitize(new_body)
        new_body = merge(new_body, self.body_patch)
        self.log.info("Calling insert instance template with updated body: {}".
                      format(new_body))
        self._hook.insert_instance_template(project_id=self.project_id,
                                            body=new_body,
                                            request_id=self.request_id)
        return self._hook.get_instance_template(project_id=self.project_id,
                                                resource_id=self.body_patch['name'])


class GceInstanceGroupManagerUpdateTemplateOperator(GceBaseOperator):
    """
    Patches the Instance Group Manager, replacing source template URL with the
    destination one. API V1 does not have update/patch operations for Instance
    Group Manager, so you must use beta or newer API version. Beta is the default.

    :param resource_id: Name of the Instance Group Manager
    :type resource_id: str
    :param zone: Google Cloud Platform zone where the Instance Group Manager exists.
    :type zone: str
    :param source_template: URL of the template to replace.
    :type source_template: str
    :param destination_template: URL of the target template.
    :type destination_template: str
    :param project_id: Optional, Google Cloud Platform Project ID where the Compute
        Engine Instance exists.
    :type project_id: str
    :param request_id: Optional, unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new instance template again).
        It should be in UUID format as defined in RFC 4122.
    :type request_id: str
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud
        Platform. Defaults to 'google_cloud_default'.
    :type gcp_conn_id: str
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :type api_version: str
    :param validate_body: Optional, If set to False, body validation is not performed.
        Defaults to False.
    :type validate_body: bool
    """
    # [START gce_igm_update_template_operator_template_fields]
    template_fields = ('project_id', 'resource_id', 'zone', 'request_id',
                       'source_template', 'destination_template',
                       'gcp_conn_id', 'api_version')
    # [END gce_igm_update_template_operator_template_fields]

    @apply_defaults
    def __init__(self,
                 resource_id,
                 zone,
                 source_template,
                 destination_template,
                 project_id=None,
                 update_policy=None,
                 request_id=None,
                 gcp_conn_id='google_cloud_default',
                 api_version='beta',
                 *args, **kwargs):
        self.zone = zone
        self.source_template = source_template
        self.destination_template = destination_template
        self.request_id = request_id
        self.update_policy = update_policy
        self._change_performed = False
        if api_version == 'v1':
            raise AirflowException("Api version v1 does not have update/patch "
                                   "operations for Instance Group Managers. Use beta"
                                   " api version or above")
        super(GceInstanceGroupManagerUpdateTemplateOperator, self).__init__(
            project_id=project_id, zone=self.zone, resource_id=resource_id,
            gcp_conn_id=gcp_conn_id, api_version=api_version, *args, **kwargs)

    def _possibly_replace_template(self, dictionary):
        # type: (dict) -> None
        if dictionary.get('instanceTemplate') == self.source_template:
            dictionary['instanceTemplate'] = self.destination_template
            self._change_performed = True

    def execute(self, context):
        old_instance_group_manager = self._hook.get_instance_group_manager(
            project_id=self.project_id,
            zone=self.zone,
            resource_id=self.resource_id)
        patch_body = {}
        if 'versions' in old_instance_group_manager:
            patch_body['versions'] = old_instance_group_manager['versions']
        if 'instanceTemplate' in old_instance_group_manager:
            patch_body['instanceTemplate'] = old_instance_group_manager['instanceTemplate']
        if self.update_policy:
            patch_body['updatePolicy'] = self.update_policy
        self._possibly_replace_template(patch_body)
        if 'versions' in patch_body:
            for version in patch_body['versions']:
                self._possibly_replace_template(version)
        if self._change_performed or self.update_policy:
            self.log.info("Calling patch instance template with updated body: {}".
                          format(patch_body))
            return self._hook.patch_instance_group_manager(
                project_id=self.project_id, zone=self.zone, resource_id=self.resource_id,
                body=patch_body, request_id=self.request_id)
        else:
            # Idempotence achieved
            return True
