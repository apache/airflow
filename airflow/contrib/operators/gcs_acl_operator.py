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
"""
This module contains Google Cloud Storage ACL entry operator.
"""
import warnings
from typing import Optional

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageBucketCreateAclEntryOperator(BaseOperator):
    """
    Creates a new ACL entry on the specified bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCloudStorageBucketCreateAclEntryOperator`

    :param bucket: Name of a bucket.
    :type bucket: str
    :param entity: The entity holding the permission, in one of the following forms:
        user-userId, user-email, group-groupId, group-email, domain-domain,
        project-team-projectId, allUsers, allAuthenticatedUsers
    :type entity: str
    :param role: The access permission for the entity.
        Acceptable values are: "OWNER", "READER", "WRITER".
    :type role: str
    :param user_project: (Optional) The project to be billed for this request.
        Required for Requester Pays buckets.
    :type user_project: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    """
    # [START gcs_bucket_create_acl_template_fields]
    template_fields = ('bucket', 'entity', 'role', 'user_project')
    # [END gcs_bucket_create_acl_template_fields]

    @apply_defaults
    def __init__(
        self,
        bucket: str,
        entity: str,
        role: str,
        user_project: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        google_cloud_storage_conn_id: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.bucket = bucket
        self.entity = entity
        self.role = role
        self.user_project = user_project
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcp_conn_id
        )
        hook.insert_bucket_acl(bucket_name=self.bucket, entity=self.entity, role=self.role,
                               user_project=self.user_project)


class GoogleCloudStorageObjectCreateAclEntryOperator(BaseOperator):
    """
    Creates a new ACL entry on the specified object.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCloudStorageObjectCreateAclEntryOperator`

    :param bucket: Name of a bucket.
    :type bucket: str
    :param object_name: Name of the object. For information about how to URL encode object
        names to be path safe, see:
        https://cloud.google.com/storage/docs/json_api/#encoding
    :type object_name: str
    :param entity: The entity holding the permission, in one of the following forms:
        user-userId, user-email, group-groupId, group-email, domain-domain,
        project-team-projectId, allUsers, allAuthenticatedUsers
    :type entity: str
    :param role: The access permission for the entity.
        Acceptable values are: "OWNER", "READER".
    :type role: str
    :param generation: Optional. If present, selects a specific revision of this object.
    :type generation: long
    :param user_project: (Optional) The project to be billed for this request.
        Required for Requester Pays buckets.
    :type user_project: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    """
    # [START gcs_object_create_acl_template_fields]
    template_fields = ('bucket', 'object_name', 'entity', 'generation', 'role', 'user_project')
    # [END gcs_object_create_acl_template_fields]

    @apply_defaults
    def __init__(self,
                 bucket: str,
                 object_name: str,
                 entity: str,
                 role: str,
                 generation: Optional[int] = None,
                 user_project: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 google_cloud_storage_conn_id: Optional[str] = None,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.bucket = bucket
        self.object_name = object_name
        self.entity = entity
        self.role = role
        self.generation = generation
        self.user_project = user_project
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcp_conn_id
        )
        hook.insert_object_acl(bucket_name=self.bucket,
                               object_name=self.object_name,
                               entity=self.entity,
                               role=self.role,
                               generation=self.generation,
                               user_project=self.user_project)
