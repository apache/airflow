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

from typing import Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud import datacatalog
from google.cloud.datacatalog import (
    CreateTagRequest,
    DataCatalogClient,
    Entry,
    EntryGroup,
    SearchCatalogRequest,
    Tag,
    TagTemplate,
    TagTemplateField,
)
from google.protobuf.field_mask_pb2 import FieldMask

from airflow import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook


class CloudDataCatalogHook(GoogleBaseHook):
    """
    Hook for Google Cloud Data Catalog Service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self._client: DataCatalogClient | None = None

    def get_conn(self) -> DataCatalogClient:
        """Retrieves client library object that allow access to Cloud Data Catalog service."""
        if not self._client:
            self._client = DataCatalogClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def create_entry(
        self,
        location: str,
        entry_group: str,
        entry_id: str,
        entry: dict | Entry,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Entry:
        """
        Creates an entry.

        Currently only entries of 'FILESET' type can be created.

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
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}"
        self.log.info("Creating a new entry: parent=%s", parent)
        result = client.create_entry(
            request={"parent": parent, "entry_id": entry_id, "entry": entry},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("Created a entry: name=%s", result.name)
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_entry_group(
        self,
        location: str,
        entry_group_id: str,
        entry_group: dict | EntryGroup,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> EntryGroup:
        """
        Creates an EntryGroup.

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
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{location}"
        self.log.info("Creating a new entry group: parent=%s", parent)

        result = client.create_entry_group(
            request={"parent": parent, "entry_group_id": entry_group_id, "entry_group": entry_group},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("Created a entry group: name=%s", result.name)

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_tag(
        self,
        location: str,
        entry_group: str,
        entry: str,
        tag: dict | Tag,
        project_id: str = PROVIDE_PROJECT_ID,
        template_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Tag:
        """
        Creates a tag on an entry.

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
        """
        client = self.get_conn()
        if template_id:
            template_path = f"projects/{project_id}/locations/{location}/tagTemplates/{template_id}"
            if isinstance(tag, Tag):
                tag.template = template_path
            else:
                tag["template"] = template_path
        parent = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}/entries/{entry}"

        self.log.info("Creating a new tag: parent=%s", parent)
        # HACK: google-cloud-datacatalog has problems with mapping messages where the value is not a
        # primitive type, so we need to convert it manually.
        # See: https://github.com/googleapis/python-datacatalog/issues/84
        if isinstance(tag, dict):
            tag = Tag(
                name=tag.get("name"),
                template=tag.get("template"),
                template_display_name=tag.get("template_display_name"),
                column=tag.get("column"),
                fields={
                    k: datacatalog.TagField(**v) if isinstance(v, dict) else v
                    for k, v in tag.get("fields", {}).items()
                },
            )
        request = CreateTagRequest(
            parent=parent,
            tag=tag,
        )

        result = client.create_tag(request=request, retry=retry, timeout=timeout, metadata=metadata or ())
        self.log.info("Created a tag: name=%s", result.name)

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_tag_template(
        self,
        location,
        tag_template_id: str,
        tag_template: dict | TagTemplate,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> TagTemplate:
        """
        Creates a tag template.

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
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{location}"

        self.log.info("Creating a new tag template: parent=%s", parent)
        # HACK: google-cloud-datacatalog has problems with mapping messages where the value is not a
        # primitive type, so we need to convert it manually.
        # See: https://github.com/googleapis/python-datacatalog/issues/84
        if isinstance(tag_template, dict):
            tag_template = datacatalog.TagTemplate(
                name=tag_template.get("name"),
                display_name=tag_template.get("display_name"),
                fields={
                    k: datacatalog.TagTemplateField(**v) if isinstance(v, dict) else v
                    for k, v in tag_template.get("fields", {}).items()
                },
            )

        request = datacatalog.CreateTagTemplateRequest(
            parent=parent, tag_template_id=tag_template_id, tag_template=tag_template
        )
        result = client.create_tag_template(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("Created a tag template: name=%s", result.name)

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_tag_template_field(
        self,
        location: str,
        tag_template: str,
        tag_template_field_id: str,
        tag_template_field: dict | TagTemplateField,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> TagTemplateField:
        r"""
        Creates a field in a tag template.

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
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template}"

        self.log.info("Creating a new tag template field: parent=%s", parent)

        result = client.create_tag_template_field(
            request={
                "parent": parent,
                "tag_template_field_id": tag_template_field_id,
                "tag_template_field": tag_template_field,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Created a tag template field: name=%s", result.name)

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_entry(
        self,
        location: str,
        entry_group: str,
        entry: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes an existing entry.

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
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}/entries/{entry}"
        self.log.info("Deleting a entry: name=%s", name)
        client.delete_entry(request={"name": name}, retry=retry, timeout=timeout, metadata=metadata or ())
        self.log.info("Deleted a entry: name=%s", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_entry_group(
        self,
        location,
        entry_group,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes an EntryGroup.

        Only entry groups that do not contain entries can be deleted.

        :param location: Required. The location of the entry group to delete.
        :param entry_group: Entry group ID that is deleted.
        :param project_id: The ID of the Google Cloud project that owns the entry group.
            If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
            retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}"

        self.log.info("Deleting a entry group: name=%s", name)
        client.delete_entry_group(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata or ()
        )
        self.log.info("Deleted a entry group: name=%s", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_tag(
        self,
        location: str,
        entry_group: str,
        entry: str,
        tag: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes a tag.

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
        """
        client = self.get_conn()
        name = (
            f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}/entries/{entry}/tags/{tag}"
        )

        self.log.info("Deleting a tag: name=%s", name)
        client.delete_tag(request={"name": name}, retry=retry, timeout=timeout, metadata=metadata or ())
        self.log.info("Deleted a tag: name=%s", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_tag_template(
        self,
        location,
        tag_template,
        force: bool,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes a tag template and all tags using the template.

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
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template}"

        self.log.info("Deleting a tag template: name=%s", name)
        client.delete_tag_template(
            request={"name": name, "force": force}, retry=retry, timeout=timeout, metadata=metadata or ()
        )
        self.log.info("Deleted a tag template: name=%s", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_tag_template_field(
        self,
        location: str,
        tag_template: str,
        field: str,
        force: bool,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Deletes a field in a tag template and all uses of that field.

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
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template}/fields/{field}"

        self.log.info("Deleting a tag template field: name=%s", name)
        client.delete_tag_template_field(
            request={"name": name, "force": force}, retry=retry, timeout=timeout, metadata=metadata or ()
        )
        self.log.info("Deleted a tag template field: name=%s", name)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_entry(
        self,
        location: str,
        entry_group: str,
        entry: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Entry:
        """
        Gets an entry.

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
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}/entries/{entry}"

        self.log.info("Getting a entry: name=%s", name)
        result = client.get_entry(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata or ()
        )
        self.log.info("Received a entry: name=%s", result.name)

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_entry_group(
        self,
        location: str,
        entry_group: str,
        project_id: str,
        read_mask: FieldMask | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> EntryGroup:
        """
        Gets an entry group.

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
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}"

        self.log.info("Getting a entry group: name=%s", name)

        result = client.get_entry_group(
            request={"name": name, "read_mask": read_mask},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Received a entry group: name=%s", result.name)

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_tag_template(
        self,
        location: str,
        tag_template: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> TagTemplate:
        """
        Gets a tag template.

        :param location: Required. The location of the tag template to get.
        :param tag_template: Required. The ID of the tag template to get.
        :param project_id: The ID of the Google Cloud project that owns the entry group.
            If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
            retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template}"

        self.log.info("Getting a tag template: name=%s", name)

        result = client.get_tag_template(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata or ()
        )

        self.log.info("Received a tag template: name=%s", result.name)

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_tags(
        self,
        location: str,
        entry_group: str,
        entry: str,
        project_id: str,
        page_size: int = 100,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Lists the tags on an Entry.

        :param location: Required. The location of the tags to get.
        :param entry_group: Required. The entry group of the tags to get.
        :param entry_group: Required. The entry of the tags to get.
        :param page_size: The maximum number of resources contained in the underlying API response. If page
            streaming is performed per- resource, this parameter does not affect the return value. If page
            streaming is performed per-page, this determines the maximum number of resources in a page.
        :param project_id: The ID of the Google Cloud project that owns the entry group.
            If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
            retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_conn()
        parent = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}/entries/{entry}"

        self.log.info("Listing tag on entry: entry_name=%s", parent)

        result = client.list_tags(
            request={"parent": parent, "page_size": page_size},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Received tags.")

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_tag_for_template_name(
        self,
        location: str,
        entry_group: str,
        entry: str,
        template_name: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Tag:
        """
        Gets for a tag with a specific template for a specific entry.

        :param location: Required. The location which contains the entry to search for.
        :param entry_group: The entry group ID which contains the entry to search for.
        :param entry:  The name of the entry to search for.
        :param template_name: The name of the template that will be the search criterion.
        :param project_id: The ID of the Google Cloud project that owns the entry group.
            If set to ``None`` or missing, the default project_id from the Google Cloud connection is used.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
            retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        tags_list = self.list_tags(
            location=location,
            entry_group=entry_group,
            entry=entry,
            project_id=project_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        tag = next(t for t in tags_list if t.template == template_name)
        return tag

    def lookup_entry(
        self,
        linked_resource: str | None = None,
        sql_resource: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Entry:
        r"""
        Get an entry by target resource name.

        This method allows clients to use the resource name from the source Google Cloud service
        to get the Data Catalog Entry.

        :param linked_resource: The full name of the Google Cloud resource the Data Catalog entry
            represents. See: https://cloud.google.com/apis/design/resource\_names#full\_resource\_name. Full
            names are case-sensitive.

        :param sql_resource: The SQL name of the entry. SQL names are case-sensitive.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will be
            retried using a default configuration.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_conn()
        if linked_resource and sql_resource:
            raise AirflowException("Only one of linked_resource, sql_resource should be set.")

        if not linked_resource and not sql_resource:
            raise AirflowException("At least one of linked_resource, sql_resource should be set.")

        if linked_resource:
            self.log.info("Getting entry: linked_resource=%s", linked_resource)
            result = client.lookup_entry(
                request={"linked_resource": linked_resource},
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            )
        else:
            self.log.info("Getting entry: sql_resource=%s", sql_resource)
            result = client.lookup_entry(
                request={"sql_resource": sql_resource},
                retry=retry,
                timeout=timeout,
                metadata=metadata,
            )
        self.log.info("Received entry. name=%s", result.name)

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def rename_tag_template_field(
        self,
        location: str,
        tag_template: str,
        field: str,
        new_tag_template_field_id: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> TagTemplateField:
        """
        Renames a field in a tag template.

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
        """
        client = self.get_conn()
        name = f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template}/fields/{field}"

        self.log.info(
            "Renaming field: old_name=%s, new_tag_template_field_id=%s", name, new_tag_template_field_id
        )

        result = client.rename_tag_template_field(
            request={"name": name, "new_tag_template_field_id": new_tag_template_field_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Renamed tag template field.")

        return result

    def search_catalog(
        self,
        scope: dict | SearchCatalogRequest.Scope,
        query: str,
        page_size: int = 100,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        r"""
        Searches Data Catalog for multiple resources like entries, tags that match a query.

        This does not return the complete resource, only the resource identifier and high level fields.
        Clients can subsequently call ``Get`` methods.

        Note that searches do not have full recall. There may be results that match your query but are not
        returned, even in subsequent pages of results. These missing results may vary across repeated calls to
        search. Do not rely on this method if you need to guarantee full recall.

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
        """
        client = self.get_conn()

        self.log.info(
            "Searching catalog: scope=%s, query=%s, page_size=%s, order_by=%s",
            scope,
            query,
            page_size,
            order_by,
        )
        result = client.search_catalog(
            request={"scope": scope, "query": query, "page_size": page_size, "order_by": order_by},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Received items.")

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_entry(
        self,
        entry: dict | Entry,
        update_mask: dict | FieldMask,
        project_id: str,
        location: str | None = None,
        entry_group: str | None = None,
        entry_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Entry:
        """
        Updates an existing entry.

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
        """
        client = self.get_conn()
        if project_id and location and entry_group and entry_id:
            full_entry_name = (
                f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}/entries/{entry_id}"
            )
            if isinstance(entry, Entry):
                entry.name = full_entry_name
            elif isinstance(entry, dict):
                entry["name"] = full_entry_name
            else:
                raise AirflowException("Unable to set entry's name.")
        elif location and entry_group and entry_id:
            raise AirflowException(
                "You must provide all the parameters (project_id, location, entry_group, entry_id) "
                "contained in the name, or do not specify any parameters and pass the name on the object "
            )
        name = entry.name if isinstance(entry, Entry) else entry["name"]
        self.log.info("Updating entry: name=%s", name)

        # HACK: google-cloud-datacatalog has a problem with dictionaries for update methods.
        if isinstance(entry, dict):
            entry = Entry(**entry)
        result = client.update_entry(
            request={"entry": entry, "update_mask": update_mask},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        self.log.info("Updated entry.")

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_tag(
        self,
        tag: dict | Tag,
        update_mask: dict | FieldMask,
        project_id: str,
        location: str | None = None,
        entry_group: str | None = None,
        entry: str | None = None,
        tag_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Tag:
        """
        Updates an existing tag.

        :param tag: Required. The updated tag. The "name" field must be set.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.datacatalog_v1beta1.types.Tag`
        :param update_mask: The fields to update on the Tag. If absent or empty, all modifiable fields are
            updated. Currently the only modifiable field is the field ``fields``.

            If a dict is provided, it must be of the same form as the protobuf message
            :class:`~google.cloud.datacatalog_v1beta1.types.FieldMask`
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
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_conn()
        if project_id and location and entry_group and entry and tag_id:
            full_tag_name = (
                f"projects/{project_id}/locations/{location}/entryGroups/{entry_group}/entries/{entry}"
                f"/tags/{tag_id}"
            )
            if isinstance(tag, Tag):
                tag.name = full_tag_name
            elif isinstance(tag, dict):
                tag["name"] = full_tag_name
            else:
                raise AirflowException("Unable to set tag's name.")
        elif location and entry_group and entry and tag_id:
            raise AirflowException(
                "You must provide all the parameters (project_id, location, entry_group, entry, tag_id) "
                "contained in the name, or do not specify any parameters and pass the name on the object "
            )

        name = tag.name if isinstance(tag, Tag) else tag["name"]
        self.log.info("Updating tag: name=%s", name)

        # HACK: google-cloud-datacatalog has a problem with dictionaries for update methods.
        if isinstance(tag, dict):
            tag = Tag(**tag)
        result = client.update_tag(
            request={"tag": tag, "update_mask": update_mask},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("Updated tag.")

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_tag_template(
        self,
        tag_template: dict | TagTemplate,
        update_mask: dict | FieldMask,
        project_id: str,
        location: str | None = None,
        tag_template_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> TagTemplate:
        """
        Updates a tag template.

        This method cannot be used to update the fields of a template. The tag
        template fields are represented as separate resources and should be updated using their own
        create/update/delete methods.

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
        """
        client = self.get_conn()
        if project_id and location and tag_template:
            full_tag_template_name = (
                f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template_id}"
            )
            if isinstance(tag_template, TagTemplate):
                tag_template.name = full_tag_template_name
            elif isinstance(tag_template, dict):
                tag_template["name"] = full_tag_template_name
            else:
                raise AirflowException("Unable to set name of tag template.")
        elif location and tag_template:
            raise AirflowException(
                "You must provide all the parameters (project_id, location, tag_template_id) "
                "contained in the name, or do not specify any parameters and pass the name on the object "
            )

        name = tag_template.name if isinstance(tag_template, TagTemplate) else tag_template["name"]
        self.log.info("Updating tag template: name=%s", name)

        # HACK: google-cloud-datacatalog has a problem with dictionaries for update methods.
        if isinstance(tag_template, dict):
            tag_template = TagTemplate(**tag_template)
        result = client.update_tag_template(
            request={"tag_template": tag_template, "update_mask": update_mask},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("Updated tag template.")

        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_tag_template_field(
        self,
        tag_template_field: dict | TagTemplateField,
        update_mask: dict | FieldMask,
        project_id: str,
        tag_template_field_name: str | None = None,
        location: str | None = None,
        tag_template: str | None = None,
        tag_template_field_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ):
        """
        Updates a field in a tag template. This method cannot be used to update the field type.

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
        """
        client = self.get_conn()
        if project_id and location and tag_template and tag_template_field_id:
            tag_template_field_name = (
                f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template}"
                f"/fields/{tag_template_field_id}"
            )

        self.log.info("Updating tag template field: name=%s", tag_template_field_name)

        result = client.update_tag_template_field(
            request={
                "name": tag_template_field_name,
                "tag_template_field": tag_template_field,
                "update_mask": update_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        self.log.info("Updated tag template field.")

        return result
