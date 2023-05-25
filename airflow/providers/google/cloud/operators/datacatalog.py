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
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from google.api_core.exceptions import AlreadyExists, NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.datacatalog import (
    DataCatalogClient,
    Entry,
    EntryGroup,
    SearchCatalogRequest,
    SearchCatalogResult,
    Tag,
    TagTemplate,
    TagTemplateField,
)
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.providers.google.cloud.hooks.datacatalog import CloudDataCatalogHook
from airflow.providers.google.cloud.links.datacatalog import (
    DataCatalogEntryGroupLink,
    DataCatalogEntryLink,
    DataCatalogTagTemplateLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CloudDataCatalogCreateEntryOperator(GoogleCloudBaseOperator):
    """
    Creates an entry.

    Currently only entries of 'FILESET' type can be created.

    The newly created entry ID are saved under the ``entry_id`` key in XCOM.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogCreateEntryOperator`

    :param location: Required. The location of the entry to create.
    :param entry_group: Required. Entry group ID under which the entry is created.
    :param entry_id: Required. The id of the entry to create.
    :param entry: Required. The entry to create.
        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.datacatalog_v1beta1.types.Entry`
    :param project_id: The ID of the Google Cloud project that owns the entry.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If set to ``None`` or missing, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "entry_group",
        "entry_id",
        "entry",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogEntryLink(),)

    def __init__(
        self,
        *,
        location: str,
        entry_group: str,
        entry_id: str,
        entry: dict | Entry,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.entry_group = entry_group
        self.entry_id = entry_id
        self.entry = entry
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        try:
            result = hook.create_entry(
                location=self.location,
                entry_group=self.entry_group,
                entry_id=self.entry_id,
                entry=self.entry,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            self.log.info("Entry already exists. Skipping create operation.")
            result = hook.get_entry(
                location=self.location,
                entry_group=self.entry_group,
                entry=self.entry_id,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        _, _, entry_id = result.name.rpartition("/")
        self.log.info("Current entry_id ID: %s", entry_id)
        self.xcom_push(context, key="entry_id", value=entry_id)
        DataCatalogEntryLink.persist(
            context=context,
            task_instance=self,
            entry_id=self.entry_id,
            entry_group_id=self.entry_group,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return Entry.to_dict(result)


class CloudDataCatalogCreateEntryGroupOperator(GoogleCloudBaseOperator):
    """
    Creates an EntryGroup.

    The newly created entry group ID are saved under the ``entry_group_id`` key in XCOM.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogCreateEntryGroupOperator`

    :param location: Required. The location of the entry group to create.
    :param entry_group_id: Required. The id of the entry group to create. The id must begin with a letter
        or underscore, contain only English letters, numbers and underscores, and be at most 64
        characters.
    :param entry_group: The entry group to create. Defaults to an empty entry group.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.datacatalog_v1beta1.types.EntryGroup`
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "entry_group_id",
        "entry_group",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogEntryGroupLink(),)

    def __init__(
        self,
        *,
        location: str,
        entry_group_id: str,
        entry_group: dict | EntryGroup,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.entry_group_id = entry_group_id
        self.entry_group = entry_group
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        try:
            result = hook.create_entry_group(
                location=self.location,
                entry_group_id=self.entry_group_id,
                entry_group=self.entry_group,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            self.log.info("Entry already exists. Skipping create operation.")
            result = hook.get_entry_group(
                location=self.location,
                entry_group=self.entry_group_id,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )

        _, _, entry_group_id = result.name.rpartition("/")
        self.log.info("Current entry group ID: %s", entry_group_id)
        self.xcom_push(context, key="entry_group_id", value=entry_group_id)
        DataCatalogEntryGroupLink.persist(
            context=context,
            task_instance=self,
            entry_group_id=self.entry_group_id,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return EntryGroup.to_dict(result)


class CloudDataCatalogCreateTagOperator(GoogleCloudBaseOperator):
    """
    Creates a tag on an entry.

    The newly created tag ID are saved under the ``tag_id`` key in XCOM.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogCreateTagOperator`

    :param location: Required. The location of the tag to create.
    :param entry_group: Required. Entry group ID under which the tag is created.
    :param entry: Required. Entry group ID under which the tag is created.
    :param tag: Required. The tag to create.
        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.datacatalog_v1beta1.types.Tag`
    :param template_id: Required. Template ID used to create tag
    :param project_id: The ID of the Google Cloud project that owns the tag.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "entry_group",
        "entry",
        "tag",
        "template_id",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogEntryLink(),)

    def __init__(
        self,
        *,
        location: str,
        entry_group: str,
        entry: str,
        tag: dict | Tag,
        template_id: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.entry_group = entry_group
        self.entry = entry
        self.tag = tag
        self.template_id = template_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        try:
            tag = hook.create_tag(
                location=self.location,
                entry_group=self.entry_group,
                entry=self.entry,
                tag=self.tag,
                template_id=self.template_id,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            self.log.info("Tag already exists. Skipping create operation.")
            project_id = self.project_id or hook.project_id
            if project_id is None:
                raise RuntimeError("The project id must be set here")
            if self.template_id:
                template_name = DataCatalogClient.tag_template_path(
                    project_id, self.location, self.template_id
                )
            else:
                if isinstance(self.tag, Tag):
                    template_name = self.tag.template
                else:
                    template_name = self.tag["template"]

            tag = hook.get_tag_for_template_name(
                location=self.location,
                entry_group=self.entry_group,
                template_name=template_name,
                entry=self.entry,
                project_id=project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )

        _, _, tag_id = tag.name.rpartition("/")
        self.log.info("Current Tag ID: %s", tag_id)
        self.xcom_push(context, key="tag_id", value=tag_id)
        DataCatalogEntryLink.persist(
            context=context,
            task_instance=self,
            entry_id=self.entry,
            entry_group_id=self.entry_group,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return Tag.to_dict(tag)


class CloudDataCatalogCreateTagTemplateOperator(GoogleCloudBaseOperator):
    """
    Creates a tag template.

    The newly created tag template are saved under the ``tag_template_id`` key in XCOM.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogCreateTagTemplateOperator`

    :param location: Required. The location of the tag template to create.
    :param tag_template_id: Required. The id of the tag template to create.
    :param tag_template: Required. The tag template to create.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.datacatalog_v1beta1.types.TagTemplate`
    :param project_id: The ID of the Google Cloud project that owns the tag template.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "tag_template_id",
        "tag_template",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogTagTemplateLink(),)

    def __init__(
        self,
        *,
        location: str,
        tag_template_id: str,
        tag_template: dict | TagTemplate,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.tag_template_id = tag_template_id
        self.tag_template = tag_template
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        try:
            result = hook.create_tag_template(
                location=self.location,
                tag_template_id=self.tag_template_id,
                tag_template=self.tag_template,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            self.log.info("Tag Template already exists. Skipping create operation.")
            result = hook.get_tag_template(
                location=self.location,
                tag_template=self.tag_template_id,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        _, _, tag_template = result.name.rpartition("/")
        self.log.info("Current Tag ID: %s", tag_template)
        self.xcom_push(context, key="tag_template_id", value=tag_template)
        DataCatalogTagTemplateLink.persist(
            context=context,
            task_instance=self,
            tag_template_id=self.tag_template_id,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return TagTemplate.to_dict(result)


class CloudDataCatalogCreateTagTemplateFieldOperator(GoogleCloudBaseOperator):
    r"""
    Creates a field in a tag template.

    The newly created tag template field are saved under the ``tag_template_field_id`` key in XCOM.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogCreateTagTemplateFieldOperator`

    :param location: Required. The location of the tag template field to create.
    :param tag_template: Required. The id of the tag template to create.
    :param tag_template_field_id: Required. The ID of the tag template field to create. Field ids can
        contain letters (both uppercase and lowercase), numbers (0-9), underscores (\_) and dashes (-).
        Field IDs must be at least 1 character long and at most 128 characters long. Field IDs must also
        be unique within their template.
    :param tag_template_field: Required. The tag template field to create.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.datacatalog_v1beta1.types.TagTemplateField`
    :param project_id: The ID of the Google Cloud project that owns the tag template field.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "tag_template",
        "tag_template_field_id",
        "tag_template_field",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogTagTemplateLink(),)

    def __init__(
        self,
        *,
        location: str,
        tag_template: str,
        tag_template_field_id: str,
        tag_template_field: dict | TagTemplateField,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.tag_template = tag_template
        self.tag_template_field_id = tag_template_field_id
        self.tag_template_field = tag_template_field
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        try:
            result = hook.create_tag_template_field(
                location=self.location,
                tag_template=self.tag_template,
                tag_template_field_id=self.tag_template_field_id,
                tag_template_field=self.tag_template_field,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            self.log.info("Tag template field already exists. Skipping create operation.")
            tag_template = hook.get_tag_template(
                location=self.location,
                tag_template=self.tag_template,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            result = tag_template.fields[self.tag_template_field_id]

        self.log.info("Current Tag ID: %s", self.tag_template_field_id)
        self.xcom_push(context, key="tag_template_field_id", value=self.tag_template_field_id)
        DataCatalogTagTemplateLink.persist(
            context=context,
            task_instance=self,
            tag_template_id=self.tag_template,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return TagTemplateField.to_dict(result)


class CloudDataCatalogDeleteEntryOperator(GoogleCloudBaseOperator):
    """
    Deletes an existing entry.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogDeleteEntryOperator`

    :param location: Required. The location of the entry to delete.
    :param entry_group: Required. Entry group ID for entries that is deleted.
    :param entry: Entry ID that is deleted.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "entry_group",
        "entry",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        location: str,
        entry_group: str,
        entry: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.entry_group = entry_group
        self.entry = entry
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        try:
            hook.delete_entry(
                location=self.location,
                entry_group=self.entry_group,
                entry=self.entry,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.info("Entry doesn't exists. Skipping.")


class CloudDataCatalogDeleteEntryGroupOperator(GoogleCloudBaseOperator):
    """
    Deletes an EntryGroup.

    Only entry groups that do not contain entries can be deleted.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogDeleteEntryGroupOperator`

    :param location: Required. The location of the entry group to delete.
    :param entry_group: Entry group ID that is deleted.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "entry_group",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        location: str,
        entry_group: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.entry_group = entry_group
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        try:
            hook.delete_entry_group(
                location=self.location,
                entry_group=self.entry_group,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.info("Entry doesn't exists. skipping")


class CloudDataCatalogDeleteTagOperator(GoogleCloudBaseOperator):
    """
    Deletes a tag.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogDeleteTagOperator`

    :param location: Required. The location of the tag to delete.
    :param entry_group: Entry group ID for tag that is deleted.
    :param entry: Entry  ID for tag that is deleted.
    :param tag: Identifier for TAG that is deleted.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "entry_group",
        "entry",
        "tag",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        location: str,
        entry_group: str,
        entry: str,
        tag: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.entry_group = entry_group
        self.entry = entry
        self.tag = tag
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        try:
            hook.delete_tag(
                location=self.location,
                entry_group=self.entry_group,
                entry=self.entry,
                tag=self.tag,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.info("Entry doesn't exists. skipping")


class CloudDataCatalogDeleteTagTemplateOperator(GoogleCloudBaseOperator):
    """
    Deletes a tag template and all tags using the template.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogDeleteTagTemplateOperator`

    :param location: Required. The location of the tag template to delete.
    :param tag_template: ID for tag template that is deleted.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param force: Required. Currently, this field must always be set to ``true``. This confirms the
        deletion of any possible tags using this template. ``force = false`` will be supported in the
        future.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "tag_template",
        "force",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        location: str,
        tag_template: str,
        force: bool,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.tag_template = tag_template
        self.force = force
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        try:
            hook.delete_tag_template(
                location=self.location,
                tag_template=self.tag_template,
                force=self.force,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.info("Tag Template doesn't exists. skipping")


class CloudDataCatalogDeleteTagTemplateFieldOperator(GoogleCloudBaseOperator):
    """
    Deletes a field in a tag template and all uses of that field.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogDeleteTagTemplateFieldOperator`

    :param location: Required. The location of the tag template to delete.
    :param tag_template: Tag Template ID for tag template field that is deleted.
    :param field: Name of field that is deleted.
    :param force: Required. This confirms the deletion of this field from any tags using this field.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "tag_template",
        "field",
        "force",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        location: str,
        tag_template: str,
        field: str,
        force: bool,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.tag_template = tag_template
        self.field = field
        self.force = force
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        try:
            hook.delete_tag_template_field(
                location=self.location,
                tag_template=self.tag_template,
                field=self.field,
                force=self.force,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except NotFound:
            self.log.info("Tag Template field doesn't exists. skipping")


class CloudDataCatalogGetEntryOperator(GoogleCloudBaseOperator):
    """
    Gets an entry.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogGetEntryOperator`

    :param location: Required. The location of the entry to get.
    :param entry_group: Required. The entry group of the entry to get.
    :param entry: The ID of the entry to get.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "entry_group",
        "entry",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogEntryLink(),)

    def __init__(
        self,
        *,
        location: str,
        entry_group: str,
        entry: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.entry_group = entry_group
        self.entry = entry
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.get_entry(
            location=self.location,
            entry_group=self.entry_group,
            entry=self.entry,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        DataCatalogEntryLink.persist(
            context=context,
            task_instance=self,
            entry_id=self.entry,
            entry_group_id=self.entry_group,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return Entry.to_dict(result)


class CloudDataCatalogGetEntryGroupOperator(GoogleCloudBaseOperator):
    """
    Gets an entry group.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogGetEntryGroupOperator`

    :param location: Required. The location of the entry group to get.
    :param entry_group: The ID of the entry group to get.
    :param read_mask: The fields to return. If not set or empty, all fields are returned.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.protobuf.field_mask_pb2.FieldMask`
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "entry_group",
        "read_mask",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogEntryGroupLink(),)

    def __init__(
        self,
        *,
        location: str,
        entry_group: str,
        read_mask: FieldMask,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.entry_group = entry_group
        self.read_mask = read_mask
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.get_entry_group(
            location=self.location,
            entry_group=self.entry_group,
            read_mask=self.read_mask,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        DataCatalogEntryGroupLink.persist(
            context=context,
            task_instance=self,
            entry_group_id=self.entry_group,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return EntryGroup.to_dict(result)


class CloudDataCatalogGetTagTemplateOperator(GoogleCloudBaseOperator):
    """
    Gets a tag template.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogGetTagTemplateOperator`

    :param location: Required. The location of the tag template to get.
    :param tag_template: Required. The ID of the tag template to get.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "tag_template",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogTagTemplateLink(),)

    def __init__(
        self,
        *,
        location: str,
        tag_template: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.tag_template = tag_template
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.get_tag_template(
            location=self.location,
            tag_template=self.tag_template,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        DataCatalogTagTemplateLink.persist(
            context=context,
            task_instance=self,
            tag_template_id=self.tag_template,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return TagTemplate.to_dict(result)


class CloudDataCatalogListTagsOperator(GoogleCloudBaseOperator):
    """
    Lists the tags on an Entry.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogListTagsOperator`

    :param location: Required. The location of the tags to get.
    :param entry_group: Required. The entry group of the tags to get.
    :param entry: Required. The entry of the tags to get.
    :param page_size: The maximum number of resources contained in the underlying API response. If page
        streaming is performed per- resource, this parameter does not affect the return value. If page
        streaming is performed per-page, this determines the maximum number of resources in a page.
        (Default: 100)
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "entry_group",
        "entry",
        "page_size",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogEntryLink(),)

    def __init__(
        self,
        *,
        location: str,
        entry_group: str,
        entry: str,
        page_size: int = 100,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.entry_group = entry_group
        self.entry = entry
        self.page_size = page_size
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> list:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.list_tags(
            location=self.location,
            entry_group=self.entry_group,
            entry=self.entry,
            page_size=self.page_size,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        DataCatalogEntryLink.persist(
            context=context,
            task_instance=self,
            entry_id=self.entry,
            entry_group_id=self.entry_group,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )
        return [Tag.to_dict(item) for item in result]


class CloudDataCatalogLookupEntryOperator(GoogleCloudBaseOperator):
    r"""
    Get an entry by target resource name.

    This method allows clients to use the resource name from the source Google Cloud service
    to get the Data Catalog Entry.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogLookupEntryOperator`

    :param linked_resource: The full name of the Google Cloud resource the Data Catalog entry
        represents. See: https://cloud.google.com/apis/design/resource\_names#full\_resource\_name. Full
        names are case-sensitive.
    :param sql_resource: The SQL name of the entry. SQL names are case-sensitive.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "linked_resource",
        "sql_resource",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogEntryLink(),)

    def __init__(
        self,
        *,
        linked_resource: str | None = None,
        sql_resource: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.linked_resource = linked_resource
        self.sql_resource = sql_resource
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.lookup_entry(
            linked_resource=self.linked_resource,
            sql_resource=self.sql_resource,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        project_id, location_id, entry_group_id, entry_id = result.name.split("/")[1::2]
        DataCatalogEntryLink.persist(
            context=context,
            task_instance=self,
            entry_id=entry_id,
            entry_group_id=entry_group_id,
            location_id=location_id,
            project_id=project_id,
        )
        return Entry.to_dict(result)


class CloudDataCatalogRenameTagTemplateFieldOperator(GoogleCloudBaseOperator):
    """
    Renames a field in a tag template.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogRenameTagTemplateFieldOperator`

    :param location: Required. The location of the tag template field to rename.
    :param tag_template: The tag template ID for field that is renamed.
    :param field: Required. The old ID of this tag template field. For example,
        ``my_old_field``.
    :param new_tag_template_field_id: Required. The new ID of this tag template field. For example,
        ``my_new_field``.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "tag_template",
        "field",
        "new_tag_template_field_id",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogTagTemplateLink(),)

    def __init__(
        self,
        *,
        location: str,
        tag_template: str,
        field: str,
        new_tag_template_field_id: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.location = location
        self.tag_template = tag_template
        self.field = field
        self.new_tag_template_field_id = new_tag_template_field_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        hook.rename_tag_template_field(
            location=self.location,
            tag_template=self.tag_template,
            field=self.field,
            new_tag_template_field_id=self.new_tag_template_field_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        DataCatalogTagTemplateLink.persist(
            context=context,
            task_instance=self,
            tag_template_id=self.tag_template,
            location_id=self.location,
            project_id=self.project_id or hook.project_id,
        )


class CloudDataCatalogSearchCatalogOperator(GoogleCloudBaseOperator):
    r"""
    Searches Data Catalog for multiple resources like entries, tags that match a query.

    This does not return the complete resource, only the resource identifier and high level fields.
    Clients can subsequently call ``Get`` methods.

    Note that searches do not have full recall. There may be results that match your query but are not
    returned, even in subsequent pages of results. These missing results may vary across repeated calls to
    search. Do not rely on this method if you need to guarantee full recall.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogSearchCatalogOperator`

    :param scope: Required. The scope of this search request.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.datacatalog_v1beta1.types.Scope`
    :param query: Required. The query string in search query syntax. The query must be non-empty.

        Query strings can be simple as "x" or more qualified as:

        -  name:x
        -  column:x
        -  description:y

        Note: Query tokens need to have a minimum of 3 characters for substring matching to work
        correctly. See `Data Catalog Search Syntax <https://cloud.google.com/data-catalog/docs/how-
        to/search-reference>`__ for more information.
    :param page_size: The maximum number of resources contained in the underlying API response. If page
        streaming is performed per-resource, this parameter does not affect the return value. If page
        streaming is performed per-page, this determines the maximum number of resources in a page.
    :param order_by: Specifies the ordering of results, currently supported case-sensitive choices are:

        -  ``relevance``, only supports descending
        -  ``last_access_timestamp [asc|desc]``, defaults to descending if not specified
        -  ``last_modified_timestamp [asc|desc]``, defaults to descending if not specified

        If not specified, defaults to ``relevance`` descending.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "scope",
        "query",
        "page_size",
        "order_by",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        scope: dict | SearchCatalogRequest.Scope,
        query: str,
        page_size: int = 100,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.scope = scope
        self.query = query
        self.page_size = page_size
        self.order_by = order_by
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> list:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.search_catalog(
            scope=self.scope,
            query=self.query,
            page_size=self.page_size,
            order_by=self.order_by,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [SearchCatalogResult.to_dict(item) for item in result]


class CloudDataCatalogUpdateEntryOperator(GoogleCloudBaseOperator):
    """
    Updates an existing entry.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogUpdateEntryOperator`

    :param entry: Required. The updated entry. The "name" field must be set.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.datacatalog_v1beta1.types.Entry`
    :param update_mask: The fields to update on the entry. If absent or empty, all modifiable fields are
        updated.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.protobuf.field_mask_pb2.FieldMask`
    :param location: Required. The location of the entry to update.
    :param entry_group: The entry group ID for the entry that is being updated.
    :param entry_id: The entry ID that is being updated.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "entry",
        "update_mask",
        "location",
        "entry_group",
        "entry_id",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogEntryLink(),)

    def __init__(
        self,
        *,
        entry: dict | Entry,
        update_mask: dict | FieldMask,
        location: str | None = None,
        entry_group: str | None = None,
        entry_id: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.entry = entry
        self.update_mask = update_mask
        self.location = location
        self.entry_group = entry_group
        self.entry_id = entry_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.update_entry(
            entry=self.entry,
            update_mask=self.update_mask,
            location=self.location,
            entry_group=self.entry_group,
            entry_id=self.entry_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        location_id, entry_group_id, entry_id = result.name.split("/")[3::2]
        DataCatalogEntryLink.persist(
            context=context,
            task_instance=self,
            entry_id=self.entry_id or entry_id,
            entry_group_id=self.entry_group or entry_group_id,
            location_id=self.location or location_id,
            project_id=self.project_id or hook.project_id,
        )


class CloudDataCatalogUpdateTagOperator(GoogleCloudBaseOperator):
    """
    Updates an existing tag.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogUpdateTagOperator`

    :param tag: Required. The updated tag. The "name" field must be set.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.datacatalog_v1beta1.types.Tag`
    :param update_mask: The fields to update on the Tag. If absent or empty, all modifiable fields are
        updated. Currently the only modifiable field is the field ``fields``.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.protobuf.field_mask_pb2.FieldMask`
    :param location: Required. The location of the tag to rename.
    :param entry_group: The entry group ID for the tag that is being updated.
    :param entry: The entry ID for the tag that is being updated.
    :param tag_id: The tag ID that is being updated.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "tag",
        "update_mask",
        "location",
        "entry_group",
        "entry",
        "tag_id",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogEntryLink(),)

    def __init__(
        self,
        *,
        tag: dict | Tag,
        update_mask: dict | FieldMask,
        location: str | None = None,
        entry_group: str | None = None,
        entry: str | None = None,
        tag_id: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tag = tag
        self.update_mask = update_mask
        self.location = location
        self.entry_group = entry_group
        self.entry = entry
        self.tag_id = tag_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.update_tag(
            tag=self.tag,
            update_mask=self.update_mask,
            location=self.location,
            entry_group=self.entry_group,
            entry=self.entry,
            tag_id=self.tag_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        location_id, entry_group_id, entry_id = result.name.split("/")[3:8:2]
        DataCatalogEntryLink.persist(
            context=context,
            task_instance=self,
            entry_id=self.entry or entry_id,
            entry_group_id=self.entry_group or entry_group_id,
            location_id=self.location or location_id,
            project_id=self.project_id or hook.project_id,
        )


class CloudDataCatalogUpdateTagTemplateOperator(GoogleCloudBaseOperator):
    """
    Updates a tag template.

    This method cannot be used to update the fields of a template. The tag
    template fields are represented as separate resources and should be updated using their own
    create/update/delete methods.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogUpdateTagTemplateOperator`

    :param tag_template: Required. The template to update. The "name" field must be set.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.datacatalog_v1beta1.types.TagTemplate`
    :param update_mask: The field mask specifies the parts of the template to overwrite.

        If absent or empty, all of the allowed fields above will be updated.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.protobuf.field_mask_pb2.FieldMask`
    :param location: Required. The location of the tag template to rename.
    :param tag_template_id: Optional. The tag template ID for the entry that is being updated.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "tag_template",
        "update_mask",
        "location",
        "tag_template_id",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogTagTemplateLink(),)

    def __init__(
        self,
        *,
        tag_template: dict | TagTemplate,
        update_mask: dict | FieldMask,
        location: str | None = None,
        tag_template_id: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tag_template = tag_template
        self.update_mask = update_mask
        self.location = location
        self.tag_template_id = tag_template_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.update_tag_template(
            tag_template=self.tag_template,
            update_mask=self.update_mask,
            location=self.location,
            tag_template_id=self.tag_template_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        location_id, tag_template_id = result.name.split("/")[3::2]
        DataCatalogTagTemplateLink.persist(
            context=context,
            task_instance=self,
            tag_template_id=self.tag_template_id or tag_template_id,
            location_id=self.location or location_id,
            project_id=self.project_id or hook.project_id,
        )


class CloudDataCatalogUpdateTagTemplateFieldOperator(GoogleCloudBaseOperator):
    """
    Updates a field in a tag template. This method cannot be used to update the field type.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataCatalogUpdateTagTemplateFieldOperator`

    :param tag_template_field: Required. The template to update.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.datacatalog_v1beta1.types.TagTemplateField`
    :param update_mask: The field mask specifies the parts of the template to be updated. Allowed fields:

        -  ``display_name``
        -  ``type.enum_type``

        If ``update_mask`` is not set or empty, all of the allowed fields above will be updated.

        When updating an enum type, the provided values will be merged with the existing values.
        Therefore, enum values can only be added, existing enum values cannot be deleted nor renamed.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.protobuf.field_mask_pb2.FieldMask`
    :param tag_template_field_name: Optional. The name of the tag template field to rename.
    :param location: Optional. The location of the tag to rename.
    :param tag_template: Optional. The tag template ID for tag template field to rename.
    :param tag_template_field_id: Optional. The ID of tag template field to rename.
    :param project_id: The ID of the Google Cloud project that owns the entry group.
        If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
        retried using a default configuration.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
        Defaults to 'google_cloud_default'.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "tag_template_field",
        "update_mask",
        "tag_template_field_name",
        "location",
        "tag_template",
        "tag_template_field_id",
        "project_id",
        "retry",
        "timeout",
        "metadata",
        "gcp_conn_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataCatalogTagTemplateLink(),)

    def __init__(
        self,
        *,
        tag_template_field: dict | TagTemplateField,
        update_mask: dict | FieldMask,
        tag_template_field_name: str | None = None,
        location: str | None = None,
        tag_template: str | None = None,
        tag_template_field_id: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tag_template_field_name = tag_template_field_name
        self.location = location
        self.tag_template = tag_template
        self.tag_template_field_id = tag_template_field_id
        self.project_id = project_id
        self.tag_template_field = tag_template_field
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = CloudDataCatalogHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        result = hook.update_tag_template_field(
            tag_template_field=self.tag_template_field,
            update_mask=self.update_mask,
            tag_template_field_name=self.tag_template_field_name,
            location=self.location,
            tag_template=self.tag_template,
            tag_template_field_id=self.tag_template_field_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        location_id, tag_template_id = result.name.split("/")[3:6:2]
        DataCatalogTagTemplateLink.persist(
            context=context,
            task_instance=self,
            tag_template_id=self.tag_template or tag_template_id,
            location_id=self.location or location_id,
            project_id=self.project_id or hook.project_id,
        )
