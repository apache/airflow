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
"""This module contains Google Compute Engine operators."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any, Sequence

from google.api_core import exceptions
from google.cloud.compute_v1.types import Instance, InstanceGroupManager, InstanceTemplate
from json_merge_patch import merge

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
from airflow.providers.google.cloud.links.compute import (
    ComputeInstanceDetailsLink,
    ComputeInstanceGroupManagerDetailsLink,
    ComputeInstanceTemplateDetailsLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.utils.field_sanitizer import GcpBodyFieldSanitizer
from airflow.providers.google.cloud.utils.field_validator import GcpBodyFieldValidator
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from google.api_core.retry import Retry

    from airflow.utils.context import Context


class ComputeEngineBaseOperator(GoogleCloudBaseOperator):
    """Abstract base operator for Google Compute Engine operators to inherit from."""

    def __init__(
        self,
        *,
        zone: str,
        resource_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.project_id = project_id
        self.zone = zone
        self.resource_id = resource_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.impersonation_chain = impersonation_chain
        self._validate_inputs()
        super().__init__(**kwargs)

    def _validate_inputs(self) -> None:
        if self.project_id == "":
            raise AirflowException("The required parameter 'project_id' is missing")
        if not self.zone:
            raise AirflowException("The required parameter 'zone' is missing")

    def execute(self, context: Context):
        pass


class ComputeEngineInsertInstanceOperator(ComputeEngineBaseOperator):
    """
    Creates an Instance in Google Compute Engine based on specified parameters.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineInsertInstanceOperator`

    :param body: Instance representation as an object. Should at least include 'name', 'machine_type',
        'disks' and 'network_interfaces' fields but doesn't include 'zone' field, as it will be specified
        in 'zone' parameter.
        Full or partial URL and can be represented as examples below:
        1. "machine_type": "projects/your-project-name/zones/your-zone/machineTypes/your-machine-type"
        2. "disk_type": "projects/your-project-name/zones/your-zone/diskTypes/your-disk-type"
        3. "subnetwork": "projects/your-project-name/regions/your-region/subnetworks/your-subnetwork"
    :param zone: Google Cloud zone where the Instance exists
    :param project_id: Google Cloud project ID where the Compute Engine Instance exists.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param resource_id: Name of the Instance. If the name of Instance is not specified in body['name'],
        the name will be taken from 'resource_id' parameter
    :param request_id: Unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new instance template again)
        It should be in UUID format as defined in RFC 4122
    :param gcp_conn_id: The connection ID used to connect to Google Cloud. Defaults to 'google_cloud_default'.
    :param api_version: API version used (for example v1 - or beta). Defaults to v1.
    :param impersonation_chain: Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    operator_extra_links = (ComputeInstanceDetailsLink(),)

    # [START gce_instance_insert_fields]
    template_fields: Sequence[str] = (
        "body",
        "project_id",
        "zone",
        "request_id",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
        "resource_id",
    )
    # [END gce_instance_insert_fields]

    def __init__(
        self,
        *,
        body: dict,
        zone: str,
        resource_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.body = body
        self.zone = zone
        self.request_id = request_id
        if "name" in body:
            resource_id = self.body["name"]
        self._field_validator = None  # Optional[GcpBodyFieldValidator]
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION, api_version=api_version
            )
        self._field_sanitizer = GcpBodyFieldSanitizer(GCE_INSTANCE_FIELDS_TO_SANITIZE)
        super().__init__(
            resource_id=resource_id,
            zone=zone,
            project_id=project_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def check_body_fields(self) -> None:
        required_params = ["machine_type", "disks", "network_interfaces"]
        for param in required_params:
            if param not in self.body:
                readable_param = param.replace("_", " ")
                raise AirflowException(
                    f"The body '{self.body}' should contain at least {readable_param} for the new operator "
                    f"in the '{param}' field. Check (google.cloud.compute_v1.types.Instance) "
                    f"for more details about body fields description."
                )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id and "name" not in self.body:
            raise AirflowException(
                "The required parameters 'resource_id' and body['name'] are missing. "
                "Please, provide at least one of them."
            )

    def _validate_all_body_fields(self) -> None:
        if self._field_validator:
            self._field_validator.validate(self.body)

    def execute(self, context: Context) -> dict:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self._validate_all_body_fields()
        self.check_body_fields()
        try:
            # Idempotence check (sort of) - we want to check if the new Instance
            # is already created and if is, then we assume it was created previously - we do
            # not check if content of the Instance is as expected.
            # We assume success if the Instance is simply present.
            existing_instance = hook.get_instance(
                resource_id=self.resource_id,
                project_id=self.project_id,
                zone=self.zone,
            )
        except exceptions.NotFound as e:
            # We actually expect to get 404 / Not Found here as the should not yet exist
            if e.code != 404:
                raise e
        else:
            self.log.info("The %s Instance already exists", self.resource_id)
            ComputeInstanceDetailsLink.persist(
                context=context,
                task_instance=self,
                location_id=self.zone,
                resource_id=self.resource_id,
                project_id=self.project_id or hook.project_id,
            )
            return Instance.to_dict(existing_instance)
        self._field_sanitizer.sanitize(self.body)
        self.log.info("Creating Instance with specified body: %s", self.body)
        hook.insert_instance(
            body=self.body,
            request_id=self.request_id,
            project_id=self.project_id,
            zone=self.zone,
        )
        self.log.info("The specified Instance has been created SUCCESSFULLY")
        new_instance = hook.get_instance(
            resource_id=self.resource_id,
            project_id=self.project_id,
            zone=self.zone,
        )
        ComputeInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            location_id=self.zone,
            resource_id=self.resource_id,
            project_id=self.project_id or hook.project_id,
        )
        return Instance.to_dict(new_instance)


class ComputeEngineInsertInstanceFromTemplateOperator(ComputeEngineBaseOperator):
    """
    Creates an Instance in Google Compute Engine based on specified parameters from existing Template.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineInsertInstanceFromTemplateOperator`

    :param body: Instance representation as object. For this Operator only 'name' parameter is required for
        creating new Instance since all other parameters will be passed through the Template.
    :param source_instance_template: Existing Instance Template that will be used as a base while creating
        new Instance. When specified, only name of new Instance should be provided as input arguments in
        'body' parameter when creating new Instance. All other parameters, such as 'machine_type', 'disks'
        and 'network_interfaces' will be passed to Instance as they are specified in the Instance Template.
        Full or partial URL and can be represented as examples below:
        1. "https://www.googleapis.com/compute/v1/projects/your-project-name/global/instanceTemplates/temp"
        2. "projects/your-project-name/global/instanceTemplates/temp"
        3. "global/instanceTemplates/temp"
    :param zone: Google Cloud zone where the instance exists.
    :param project_id: Google Cloud project ID where the Compute Engine Instance exists.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param resource_id: Name of the Instance. If the name of Instance is not specified in body['name'],
        the name will be taken from 'resource_id' parameter
    :param request_id: Unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new instance template again)
        It should be in UUID format as defined in RFC 4122
    :param gcp_conn_id: The connection ID used to connect to Google Cloud. Defaults to 'google_cloud_default'.
    :param api_version: API version used (for example v1 - or beta). Defaults to v1.
    :param impersonation_chain: Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    operator_extra_links = (ComputeInstanceDetailsLink(),)

    # [START gce_instance_insert_from_template_fields]
    template_fields: Sequence[str] = (
        "body",
        "source_instance_template",
        "project_id",
        "zone",
        "request_id",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
        "resource_id",
    )
    # [END gce_instance_insert_from_template_fields]

    def __init__(
        self,
        *,
        source_instance_template: str,
        body: dict,
        zone: str,
        resource_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.source_instance_template = source_instance_template
        self.body = body
        self.zone = zone
        if "name" in body:
            resource_id = self.body["name"]
        self.request_id = request_id
        self._field_validator = None  # Optional[GcpBodyFieldValidator]
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION, api_version=api_version
            )
        self._field_sanitizer = GcpBodyFieldSanitizer(GCE_INSTANCE_FIELDS_TO_SANITIZE)
        super().__init__(
            resource_id=resource_id,
            zone=zone,
            project_id=project_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_all_body_fields(self) -> None:
        if self._field_validator:
            self._field_validator.validate(self.body)

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id and "name" not in self.body:
            raise AirflowException(
                "The required parameters 'resource_id' and body['name'] are missing. "
                "Please, provide at least one of them."
            )

    def execute(self, context: Context) -> dict:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self._validate_all_body_fields()
        try:
            # Idempotence check (sort of) - we want to check if the new Instance
            # is already created and if is, then we assume it was created - we do
            # not check if content of the Instance is as expected.
            # We assume success if the Instance is simply present
            existing_instance = hook.get_instance(
                resource_id=self.resource_id,
                project_id=self.project_id,
                zone=self.zone,
            )
        except exceptions.NotFound as e:
            # We actually expect to get 404 / Not Found here as the template should
            # not yet exist
            if e.code != 404:
                raise e
        else:
            self.log.info("The %s Instance already exists", self.resource_id)
            ComputeInstanceDetailsLink.persist(
                context=context,
                task_instance=self,
                location_id=self.zone,
                resource_id=self.resource_id,
                project_id=self.project_id or hook.project_id,
            )
            return Instance.to_dict(existing_instance)
        self._field_sanitizer.sanitize(self.body)
        self.log.info("Creating Instance with specified body: %s", self.body)
        hook.insert_instance(
            body=self.body,
            request_id=self.request_id,
            project_id=self.project_id,
            zone=self.zone,
            source_instance_template=self.source_instance_template,
        )
        self.log.info("The specified Instance has been created SUCCESSFULLY")
        new_instance_from_template = hook.get_instance(
            resource_id=self.resource_id,
            project_id=self.project_id,
            zone=self.zone,
        )
        ComputeInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            location_id=self.zone,
            resource_id=self.resource_id,
            project_id=self.project_id or hook.project_id,
        )
        return Instance.to_dict(new_instance_from_template)


class ComputeEngineDeleteInstanceOperator(ComputeEngineBaseOperator):
    """
    Deletes an Instance in Google Compute Engine.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineDeleteInstanceOperator`

    :param project_id: Google Cloud project ID where the Compute Engine Instance exists.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param zone: Google Cloud zone where the instance exists.
    :param resource_id: Name of the Instance.
    :param request_id: Unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new instance template again)
        It should be in UUID format as defined in RFC 4122
    :param gcp_conn_id: The connection ID used to connect to Google Cloud. Defaults to 'google_cloud_default'.
    :param api_version: API version used (for example v1 - or beta). Defaults to v1.
    :param impersonation_chain: Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    # [START gce_instance_delete_template_fields]
    template_fields: Sequence[str] = (
        "zone",
        "resource_id",
        "request_id",
        "project_id",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
    )
    # [END gce_instance_delete_template_fields]

    def __init__(
        self,
        *,
        resource_id: str,
        zone: str,
        request_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.zone = zone
        self.request_id = request_id
        self.resource_id = resource_id
        self._field_validator = None  # Optional[GcpBodyFieldValidator]
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION, api_version=api_version
            )
        self._field_sanitizer = GcpBodyFieldSanitizer(GCE_INSTANCE_FIELDS_TO_SANITIZE)
        super().__init__(
            project_id=project_id,
            zone=zone,
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id:
            raise AirflowException("The required parameter 'resource_id' is missing. ")

    def execute(self, context: Context) -> None:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            # Checking if specified Instance exists and if it does, delete it
            hook.get_instance(
                resource_id=self.resource_id,
                project_id=self.project_id,
                zone=self.zone,
            )
            self.log.info("Successfully found Instance %s", self.resource_id)
            hook.delete_instance(
                resource_id=self.resource_id,
                project_id=self.project_id,
                request_id=self.request_id,
                zone=self.zone,
            )
            self.log.info("Successfully deleted Instance %s", self.resource_id)
        except exceptions.NotFound as e:
            # Expecting 404 Error in case if Instance doesn't exist.
            if e.code == 404:
                self.log.error("Instance %s doesn't exist", self.resource_id)
                raise e


class ComputeEngineStartInstanceOperator(ComputeEngineBaseOperator):
    """
    Starts an instance in Google Compute Engine.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineStartInstanceOperator`

    :param zone: Google Cloud zone where the instance exists.
    :param resource_id: Name of the Compute Engine instance resource.
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (ComputeInstanceDetailsLink(),)

    # [START gce_instance_start_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "zone",
        "resource_id",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
    )
    # [END gce_instance_start_template_fields]

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id:
            raise AirflowException("The required parameter 'resource_id' is missing. ")

    def execute(self, context: Context) -> None:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        ComputeInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            location_id=self.zone,
            resource_id=self.resource_id,
            project_id=self.project_id or hook.project_id,
        )
        hook.start_instance(zone=self.zone, resource_id=self.resource_id, project_id=self.project_id)


class ComputeEngineStopInstanceOperator(ComputeEngineBaseOperator):
    """
    Stops an instance in Google Compute Engine.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineStopInstanceOperator`

    :param zone: Google Cloud zone where the instance exists.
    :param resource_id: Name of the Compute Engine instance resource.
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (ComputeInstanceDetailsLink(),)

    # [START gce_instance_stop_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "zone",
        "resource_id",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
    )
    # [END gce_instance_stop_template_fields]

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id:
            raise AirflowException("The required parameter 'resource_id' is missing. ")

    def execute(self, context: Context) -> None:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        ComputeInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            location_id=self.zone,
            resource_id=self.resource_id,
            project_id=self.project_id or hook.project_id,
        )
        hook.stop_instance(zone=self.zone, resource_id=self.resource_id, project_id=self.project_id)


SET_MACHINE_TYPE_VALIDATION_SPECIFICATION = [
    {"name": "machineType", "regexp": "^.+$"},
]


class ComputeEngineSetMachineTypeOperator(ComputeEngineBaseOperator):
    """
    Changes the machine type for a stopped instance to the machine type specified in the request.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineSetMachineTypeOperator`

    :param zone: Google Cloud zone where the instance exists.
    :param resource_id: Name of the Compute Engine instance resource.
    :param body: Body required by the Compute Engine setMachineType API, as described in
        https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType#request-body
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :param validate_body: Optional, If set to False, body validation is not performed.
        Defaults to False.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (ComputeInstanceDetailsLink(),)

    # [START gce_instance_set_machine_type_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "zone",
        "resource_id",
        "body",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
    )
    # [END gce_instance_set_machine_type_template_fields]

    def __init__(
        self,
        *,
        zone: str,
        resource_id: str,
        body: dict,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.body = body
        self._field_validator: GcpBodyFieldValidator | None = None
        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                SET_MACHINE_TYPE_VALIDATION_SPECIFICATION, api_version=api_version
            )
        super().__init__(
            project_id=project_id,
            zone=zone,
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_all_body_fields(self) -> None:
        if self._field_validator:
            self._field_validator.validate(self.body)

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id:
            raise AirflowException("The required parameter 'resource_id' is missing. ")

    def execute(self, context: Context) -> None:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self._validate_all_body_fields()
        ComputeInstanceDetailsLink.persist(
            context=context,
            task_instance=self,
            location_id=self.zone,
            resource_id=self.resource_id,
            project_id=self.project_id or hook.project_id,
        )
        hook.set_machine_type(
            zone=self.zone, resource_id=self.resource_id, body=self.body, project_id=self.project_id
        )


GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION: list[dict[str, Any]] = [
    {"name": "name", "regexp": "^.+$"},
    {"name": "description", "optional": True},
    {
        "name": "properties",
        "type": "dict",
        "optional": True,
        "fields": [
            {"name": "description", "optional": True},
            {"name": "tags", "optional": True, "fields": [{"name": "items", "optional": True}]},
            {"name": "machineType", "optional": True},
            {"name": "canIpForward", "optional": True},
            {"name": "networkInterfaces", "optional": True},  # not validating deeper
            {"name": "disks", "optional": True},  # not validating the array deeper
            {
                "name": "metadata",
                "optional": True,
                "fields": [
                    {"name": "fingerprint", "optional": True},
                    {"name": "items", "optional": True},
                    {"name": "kind", "optional": True},
                ],
            },
            {"name": "serviceAccounts", "optional": True},  # not validating deeper
            {
                "name": "scheduling",
                "optional": True,
                "fields": [
                    {"name": "onHostMaintenance", "optional": True},
                    {"name": "automaticRestart", "optional": True},
                    {"name": "preemptible", "optional": True},
                    {"name": "nodeAffinities", "optional": True},  # not validating deeper
                ],
            },
            {"name": "labels", "optional": True},
            {"name": "guestAccelerators", "optional": True},  # not validating deeper
            {"name": "minCpuPlatform", "optional": True},
        ],
    },
]

GCE_INSTANCE_FIELDS_TO_SANITIZE = [
    "kind",
    "id",
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
    "selfLink",
]


class ComputeEngineInsertInstanceTemplateOperator(ComputeEngineBaseOperator):
    """
    Creates an Instance Template using specified fields.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineInsertInstanceTemplateOperator`

    :param body: Instance template representation as object.
    :param project_id: Google Cloud project ID where the Compute Engine Instance exists.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param request_id: Unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new instance template again)
        It should be in UUID format as defined in RFC 4122
    :param resource_id: Name of the Instance Template. If the name of Instance Template is not specified in
        body['name'], the name will be taken from 'resource_id' parameter
    :param gcp_conn_id: The connection ID used to connect to Google Cloud. Defaults to 'google_cloud_default'.
    :param api_version: API version used (for example v1 - or beta). Defaults to v1.
    :param impersonation_chain: Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    operator_extra_links = (ComputeInstanceTemplateDetailsLink(),)

    # [START gce_instance_template_insert_fields]
    template_fields: Sequence[str] = (
        "body",
        "project_id",
        "request_id",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
        "resource_id",
    )
    # [END gce_instance_template_insert_fields]

    def __init__(
        self,
        *,
        body: dict,
        project_id: str = PROVIDE_PROJECT_ID,
        resource_id: str | None = None,
        request_id: str | None = None,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.body = body
        self.request_id = request_id
        if "name" in body:
            resource_id = self.body["name"]
        self._field_validator = None  # Optional[GcpBodyFieldValidator]
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION, api_version=api_version
            )
        self._field_sanitizer = GcpBodyFieldSanitizer(GCE_INSTANCE_FIELDS_TO_SANITIZE)
        super().__init__(
            project_id=project_id,
            zone="global",
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def check_body_fields(self) -> None:
        required_params = ["machine_type", "disks", "network_interfaces"]
        for param in required_params:
            if param not in self.body["properties"]:
                readable_param = param.replace("_", " ")
                raise AirflowException(
                    f"The body '{self.body}' should contain at least {readable_param} for the new operator "
                    f"in the '{param}' field. Check (google.cloud.compute_v1.types.Instance) "
                    f"for more details about body fields description."
                )

    def _validate_all_body_fields(self) -> None:
        if self._field_validator:
            self._field_validator.validate(self.body)

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id and "name" not in self.body:
            raise AirflowException(
                "The required parameters 'resource_id' and body['name'] are missing. "
                "Please, provide at least one of them."
            )

    def execute(self, context: Context) -> dict:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self._validate_all_body_fields()
        self.check_body_fields()
        self._field_sanitizer.sanitize(self.body)
        try:
            # Idempotence check (sort of) - we want to check if the new Template
            # is already created and if is, then we assume it was created by previous run
            # of operator - we do not check if content of the Template
            # is as expected. Templates are immutable, so we cannot update it anyway
            # and deleting/recreating is not worth the hassle especially
            # that we cannot delete template if it is already used in some Instance
            # Group Manager. We assume success if the template is simply present
            existing_template = hook.get_instance_template(
                resource_id=self.resource_id, project_id=self.project_id
            )
        except exceptions.NotFound as e:
            # We actually expect to get 404 / Not Found here as the template should
            # not yet exist
            if e.code != 404:
                raise e
        else:
            self.log.info("The %s Template already exists.", existing_template)
            ComputeInstanceTemplateDetailsLink.persist(
                context=context,
                task_instance=self,
                resource_id=self.resource_id,
                project_id=self.project_id or hook.project_id,
            )
            return InstanceTemplate.to_dict(existing_template)
        self._field_sanitizer.sanitize(self.body)
        self.log.info("Creating Instance Template with specified body: %s", self.body)
        hook.insert_instance_template(
            body=self.body,
            request_id=self.request_id,
            project_id=self.project_id,
        )
        self.log.info("The specified Instance Template has been created SUCCESSFULLY", self.body)
        new_template = hook.get_instance_template(
            resource_id=self.resource_id,
            project_id=self.project_id,
        )
        ComputeInstanceTemplateDetailsLink.persist(
            context=context,
            task_instance=self,
            resource_id=self.resource_id,
            project_id=self.project_id or hook.project_id,
        )
        return InstanceTemplate.to_dict(new_template)


class ComputeEngineDeleteInstanceTemplateOperator(ComputeEngineBaseOperator):
    """
    Deletes an Instance Template in Google Compute Engine.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineDeleteInstanceTemplateOperator`

    :param resource_id: Name of the Instance Template.
    :param project_id: Google Cloud project ID where the Compute Engine Instance exists.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param request_id: Unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new instance template again)
        It should be in UUID format as defined in RFC 4122
    :param gcp_conn_id: The connection ID used to connect to Google Cloud. Defaults to 'google_cloud_default'.
    :param api_version: API version used (for example v1 - or beta). Defaults to v1.
    :param impersonation_chain: Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    # [START gce_instance_template_delete_fields]
    template_fields: Sequence[str] = (
        "resource_id",
        "request_id",
        "project_id",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
    )
    # [END gce_instance_template_delete_fields]

    def __init__(
        self,
        *,
        resource_id: str,
        request_id: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.request_id = request_id
        self.resource_id = resource_id
        self._field_validator = None  # Optional[GcpBodyFieldValidator]
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata

        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION, api_version=api_version
            )
        self._field_sanitizer = GcpBodyFieldSanitizer(GCE_INSTANCE_FIELDS_TO_SANITIZE)
        super().__init__(
            project_id=project_id,
            zone="global",
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id:
            raise AirflowException("The required parameter 'resource_id' is missing.")

    def execute(self, context: Context) -> None:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            # Checking if specified Instance Template exists and if it does, delete it
            hook.get_instance_template(
                resource_id=self.resource_id,
                project_id=self.project_id,
            )
            self.log.info("Successfully found Instance Template %s", self.resource_id)
            hook.delete_instance_template(
                resource_id=self.resource_id,
                project_id=self.project_id,
                request_id=self.request_id,
            )
            self.log.info("Successfully deleted Instance template %s", self.resource_id)
        except exceptions.NotFound as e:
            # Expecting 404 Error in case if Instance template doesn't exist.
            if e.code == 404:
                self.log.error("Instance template %s doesn't exist", self.resource_id)
                raise e


class ComputeEngineCopyInstanceTemplateOperator(ComputeEngineBaseOperator):
    """
    Copies the instance template, applying specified changes.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineCopyInstanceTemplateOperator`

    :param resource_id: Name of the Instance Template
    :param body_patch: Patch to the body of instanceTemplates object following rfc7386
        PATCH semantics. The body_patch content follows
        https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates
        Name field is required as we need to rename the template,
        all the other fields are optional. It is important to follow PATCH semantics
        - arrays are replaced fully, so if you need to update an array you should
        provide the whole target array as patch element.
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :param request_id: Optional, unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new instance template again).
        It should be in UUID format as defined in RFC 4122.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :param validate_body: Optional, If set to False, body validation is not performed.
        Defaults to False.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (ComputeInstanceTemplateDetailsLink(),)

    # [START gce_instance_template_copy_operator_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "resource_id",
        "request_id",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
    )
    # [END gce_instance_template_copy_operator_template_fields]

    def __init__(
        self,
        *,
        resource_id: str,
        body_patch: dict,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        validate_body: bool = True,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.body_patch = body_patch
        self.request_id = request_id
        self._field_validator = None  # GcpBodyFieldValidator | None
        if "name" not in self.body_patch:
            raise AirflowException(
                f"The body '{body_patch}' should contain at least name for the new operator "
                f"in the 'name' field"
            )
        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION, api_version=api_version
            )
        self._field_sanitizer = GcpBodyFieldSanitizer(GCE_INSTANCE_FIELDS_TO_SANITIZE)
        super().__init__(
            project_id=project_id,
            zone="global",
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_all_body_fields(self) -> None:
        if self._field_validator:
            self._field_validator.validate(self.body_patch)

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id:
            raise AirflowException("The required parameter 'resource_id' is missing.")

    def execute(self, context: Context) -> dict:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self._validate_all_body_fields()
        try:
            # Idempotence check (sort of) - we want to check if the new template
            # is already created and if is, then we assume it was created by previous run
            # of CopyTemplate operator - we do not check if content of the template
            # is as expected. Templates are immutable, so we cannot update it anyway
            # and deleting/recreating is not worth the hassle especially
            # that we cannot delete template if it is already used in some Instance
            # Group Manager. We assume success if the template is simply present
            existing_template = hook.get_instance_template(
                resource_id=self.body_patch["name"],
                project_id=self.project_id,
            )
        except exceptions.NotFound as e:
            # We actually expect to get 404 / Not Found here as the template should
            # not yet exist
            if e.code != 404:
                raise e
        else:
            self.log.info(
                "The %s template already exists. It was likely created by previous run of the operator. "
                "Assuming success.",
                existing_template,
            )
            ComputeInstanceTemplateDetailsLink.persist(
                context=context,
                task_instance=self,
                resource_id=self.body_patch["name"],
                project_id=self.project_id or hook.project_id,
            )
            return InstanceTemplate.to_dict(existing_template)
        old_body = InstanceTemplate.to_dict(
            hook.get_instance_template(
                resource_id=self.resource_id,
                project_id=self.project_id,
            )
        )
        new_body = deepcopy(old_body)
        self._field_sanitizer.sanitize(new_body)
        new_body = merge(new_body, self.body_patch)
        self.log.info("Calling insert instance template with updated body: %s", new_body)
        hook.insert_instance_template(body=new_body, request_id=self.request_id, project_id=self.project_id)
        new_instance_tmp = hook.get_instance_template(
            resource_id=self.body_patch["name"], project_id=self.project_id
        )
        ComputeInstanceTemplateDetailsLink.persist(
            context=context,
            task_instance=self,
            resource_id=self.body_patch["name"],
            project_id=self.project_id or hook.project_id,
        )
        return InstanceTemplate.to_dict(new_instance_tmp)


class ComputeEngineInstanceGroupUpdateManagerTemplateOperator(ComputeEngineBaseOperator):
    """
    Patches the Instance Group Manager, replacing source template URL with the destination one.

    API V1 does not have update/patch operations for Instance Group Manager,
    so you must use beta or newer API version. Beta is the default.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineInstanceGroupUpdateManagerTemplateOperator`

    :param resource_id: Name of the Instance Group Manager
    :param zone: Google Cloud zone where the Instance Group Manager exists.
    :param source_template: URL of the template to replace.
    :param destination_template: URL of the target template.
    :param project_id: Optional, Google Cloud Project ID where the Compute
        Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
        connection is used.
    :param request_id: Optional, unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new instance template again).
        It should be in UUID format as defined in RFC 4122.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param api_version: Optional, API version used (for example v1 - or beta). Defaults
        to v1.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (ComputeInstanceGroupManagerDetailsLink(),)

    # [START gce_igm_update_template_operator_template_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "resource_id",
        "zone",
        "request_id",
        "source_template",
        "destination_template",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
    )
    # [END gce_igm_update_template_operator_template_fields]

    def __init__(
        self,
        *,
        resource_id: str,
        zone: str,
        source_template: str,
        destination_template: str,
        project_id: str = PROVIDE_PROJECT_ID,
        update_policy: dict[str, Any] | None = None,
        request_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        api_version="beta",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        self.zone = zone
        self.source_template = source_template
        self.destination_template = destination_template
        self.request_id = request_id
        self.update_policy = update_policy
        self._change_performed = False
        if api_version == "v1":
            raise AirflowException(
                "Api version v1 does not have update/patch "
                "operations for Instance Group Managers. Use beta"
                " api version or above"
            )
        super().__init__(
            project_id=project_id,
            zone=zone,
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id:
            raise AirflowException("The required parameter 'resource_id' is missing. ")

    def _possibly_replace_template(self, dictionary: dict) -> None:
        if dictionary.get("instanceTemplate") == self.source_template:
            dictionary["instanceTemplate"] = self.destination_template
            self._change_performed = True

    def execute(self, context: Context) -> bool | None:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        old_instance_group_manager = hook.get_instance_group_manager(
            zone=self.zone, resource_id=self.resource_id, project_id=self.project_id
        )
        patch_body = {}
        igm_dict = InstanceGroupManager.to_dict(old_instance_group_manager)
        if "versions" in igm_dict:
            patch_body["versions"] = igm_dict["versions"]
        if "instanceTemplate" in igm_dict:
            patch_body["instanceTemplate"] = igm_dict["instanceTemplate"]
        if self.update_policy:
            patch_body["updatePolicy"] = self.update_policy
        self._possibly_replace_template(patch_body)
        if "versions" in patch_body:
            for version in patch_body["versions"]:
                self._possibly_replace_template(version)
        if self._change_performed or self.update_policy:
            self.log.info("Calling patch instance template with updated body: %s", patch_body)
            ComputeInstanceGroupManagerDetailsLink.persist(
                context=context,
                task_instance=self,
                location_id=self.zone,
                resource_id=self.resource_id,
                project_id=self.project_id or hook.project_id,
            )
            return hook.patch_instance_group_manager(
                zone=self.zone,
                resource_id=self.resource_id,
                body=patch_body,
                request_id=self.request_id,
                project_id=self.project_id,
            )
        else:
            # Idempotence achieved
            ComputeInstanceGroupManagerDetailsLink.persist(
                context=context,
                task_instance=self,
                location_id=self.zone,
                resource_id=self.resource_id,
                project_id=self.project_id or hook.project_id,
            )
            return True


class ComputeEngineInsertInstanceGroupManagerOperator(ComputeEngineBaseOperator):
    """
    Creates an Instance Group Managers using the body specified.

    After the group is created, instances in the group are created using the specified Instance Template.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineInsertInstanceGroupManagerOperator`

    :param body: Instance Group Managers representation as object.
    :param project_id: Google Cloud project ID where the Compute Engine Instance Group Managers exists.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param request_id: Unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new Instance Group Managers again)
        It should be in UUID format as defined in RFC 4122
    :param resource_id: Name of the Instance Group Managers. If the name of Instance Group Managers is
        not specified in body['name'], the name will be taken from 'resource_id' parameter.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud. Defaults to 'google_cloud_default'.
    :param api_version: API version used (for example v1 - or beta). Defaults to v1.
    :param impersonation_chain: Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    operator_extra_links = (ComputeInstanceGroupManagerDetailsLink(),)

    # [START gce_igm_insert_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "body",
        "zone",
        "request_id",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
        "resource_id",
    )
    # [END gce_igm_insert_fields]

    def __init__(
        self,
        *,
        body: dict,
        zone: str,
        project_id: str = PROVIDE_PROJECT_ID,
        resource_id: str | None = None,
        request_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        api_version="v1",
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        validate_body: bool = True,
        **kwargs,
    ) -> None:
        self.body = body
        self.request_id = request_id
        if "name" in body:
            resource_id = self.body["name"]
        self._field_validator = None  # Optional[GcpBodyFieldValidator]
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION, api_version=api_version
            )
        self._field_sanitizer = GcpBodyFieldSanitizer(GCE_INSTANCE_FIELDS_TO_SANITIZE)
        super().__init__(
            project_id=project_id,
            zone=zone,
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def check_body_fields(self) -> None:
        required_params = ["base_instance_name", "target_size", "instance_template"]
        for param in required_params:
            if param not in self.body:
                readable_param = param.replace("_", " ")
                raise AirflowException(
                    f"The body '{self.body}' should contain at least {readable_param} for the new operator "
                    f"in the '{param}' field. Check (google.cloud.compute_v1.types.Instance) "
                    f"for more details about body fields description."
                )

    def _validate_all_body_fields(self) -> None:
        if self._field_validator:
            self._field_validator.validate(self.body)

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id and "name" not in self.body:
            raise AirflowException(
                "The required parameters 'resource_id' and body['name'] are missing. "
                "Please, provide at least one of them."
            )

    def execute(self, context: Context) -> dict:
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self._validate_all_body_fields()
        self.check_body_fields()
        try:
            # Idempotence check (sort of) - we want to check if the new Instance Group Manager
            # is already created and if isn't, we create new one
            existing_instance_group_manager = hook.get_instance_group_manager(
                resource_id=self.resource_id,
                project_id=self.project_id,
                zone=self.zone,
            )
        except exceptions.NotFound as e:
            # We actually expect to get 404 / Not Found here as the Instance Group Manager should
            # not yet exist
            if e.code != 404:
                raise e
        else:
            self.log.info("The %s Instance Group Manager already exists", existing_instance_group_manager)
            ComputeInstanceGroupManagerDetailsLink.persist(
                context=context,
                task_instance=self,
                resource_id=self.resource_id,
                project_id=self.project_id or hook.project_id,
                location_id=self.zone,
            )
            return InstanceGroupManager.to_dict(existing_instance_group_manager)
        self._field_sanitizer.sanitize(self.body)
        self.log.info("Creating Instance Group Manager with specified body: %s", self.body)
        hook.insert_instance_group_manager(
            body=self.body,
            request_id=self.request_id,
            project_id=self.project_id,
            zone=self.zone,
        )
        self.log.info("The specified Instance Group Manager has been created SUCCESSFULLY", self.body)
        new_instance_group_manager = hook.get_instance_group_manager(
            resource_id=self.resource_id,
            project_id=self.project_id,
            zone=self.zone,
        )
        ComputeInstanceGroupManagerDetailsLink.persist(
            context=context,
            task_instance=self,
            location_id=self.zone,
            resource_id=self.resource_id,
            project_id=self.project_id or hook.project_id,
        )
        return InstanceGroupManager.to_dict(new_instance_group_manager)


class ComputeEngineDeleteInstanceGroupManagerOperator(ComputeEngineBaseOperator):
    """
    Permanently and irrevocably deletes an Instance Group Managers.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ComputeEngineDeleteInstanceGroupManagerOperator`

    :param resource_id: Name of the Instance Group Managers.
    :param project_id: Google Cloud project ID where the Compute Engine Instance Group Managers exists.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param request_id: Unique request_id that you might add to achieve
        full idempotence (for example when client call times out repeating the request
        with the same request id will not create a new Instance Group Managers again)
        It should be in UUID format as defined in RFC 4122
    :param gcp_conn_id: The connection ID used to connect to Google Cloud. Defaults to 'google_cloud_default'.
    :param api_version: API version used (for example v1 - or beta). Defaults to v1.
    :param impersonation_chain: Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    # [START gce_igm_delete_fields]
    template_fields: Sequence[str] = (
        "project_id",
        "resource_id",
        "zone",
        "request_id",
        "gcp_conn_id",
        "api_version",
        "impersonation_chain",
    )
    # [END gce_igm_delete_fields]

    def __init__(
        self,
        *,
        resource_id: str,
        zone: str,
        project_id: str = PROVIDE_PROJECT_ID,
        request_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        api_version="v1",
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        validate_body: bool = True,
        **kwargs,
    ) -> None:
        self.zone = zone
        self.request_id = request_id
        self.resource_id = resource_id
        self._field_validator = None  # Optional[GcpBodyFieldValidator]
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        if validate_body:
            self._field_validator = GcpBodyFieldValidator(
                GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION, api_version=api_version
            )
        self._field_sanitizer = GcpBodyFieldSanitizer(GCE_INSTANCE_FIELDS_TO_SANITIZE)
        super().__init__(
            project_id=project_id,
            zone=zone,
            resource_id=resource_id,
            gcp_conn_id=gcp_conn_id,
            api_version=api_version,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    def _validate_inputs(self) -> None:
        super()._validate_inputs()
        if not self.resource_id:
            raise AirflowException("The required parameter 'resource_id' is missing. ")

    def execute(self, context: Context):
        hook = ComputeEngineHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            # Checking if specified Instance Group Managers exists and if it does, delete it
            hook.get_instance_group_manager(
                resource_id=self.resource_id,
                project_id=self.project_id,
                zone=self.zone,
            )
            self.log.info("Successfully found Group Manager %s", self.resource_id)
            hook.delete_instance_group_manager(
                resource_id=self.resource_id,
                project_id=self.project_id,
                request_id=self.request_id,
                zone=self.zone,
            )
            self.log.info("Successfully deleted Instance Group Managers")
        except exceptions.NotFound as e:
            # Expecting 404 Error in case if Instance Group Managers doesn't exist.
            if e.code == 404:
                self.log.error("Instance Group Managers %s doesn't exist", self.resource_id)
                raise e
