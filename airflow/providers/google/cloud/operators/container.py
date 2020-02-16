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

from typing import Dict, Optional

from google.api_core.gapic_v1.method import DEFAULT

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.container import ContainerHook
from airflow.utils.decorators import apply_defaults


class ContainerCreateGKEClusterOperator(BaseOperator):
    template_fields = ()
    ui_color = ""

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        zone: str,
        cluster: Dict,
        wait_for_completion: Optional[bool] = True,
        project_id: Optional[str] = None,
        parent: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: Optional[str] = 'google_cloud_default',
        delegates_to: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.hook = None
        self.zone = zone
        self.cluster = cluster
        self.wait_for_completion = wait_for_completion
        self.project_id = project_id
        self.parent = parent
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegates_to = delegates_to

    def execute(self, context):
        if self.hook is None:
            self.hook = ContainerHook(gcp_conn_id=self.gcp_conn_id, delegates_to=self.delegates_to)
        self.hook.create_gke_cluster(
            zone=self.zone,
            cluster=self.cluster,
            wait_for_completion=self.wait_for_completion,
            project_id=self.project_id,
            parent=self.parent,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class ContainerCreateNodePoolOperator(BaseOperator):
    template_fields = ()
    ui_color = ""

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        zone: str,
        cluster_id: str,
        node_pool: Dict,
        wait_for_completion: Optional[bool] = True,
        project_id: Optional[str] = None,
        parent: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: Optional[str] = 'google_cloud_default',
        delegates_to: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.hook = None
        self.zone = zone
        self.cluster_id = cluster_id
        self.node_pool = node_pool
        self.wait_for_completion = wait_for_completion
        self.project_id = project_id
        self.parent = parent
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegates_to = delegates_to

    def execute(self, context):
        if self.hook is None:
            self.hook = ContainerHook(gcp_conn_id=self.gcp_conn_id, delegates_to=self.delegates_to)
        self.hook.create_node_pool(
            project_id=self.project_id,
            zone=self.zone,
            cluster_id=self.cluster_id,
            wait_for_completion=self.wait_for_completion,
            node_pool=self.node_pool,
            parent=self.parent,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class ContainerDeleteGKEClusterOperator(BaseOperator):
    template_fields = ()
    ui_color = ""

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        zone: str,
        cluster_id: str,
        name: Optional[str] = None,
        wait_for_completion: Optional[bool] = True,
        project_id: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: Optional[str] = 'google_cloud_default',
        delegates_to: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.hook = None
        self.zone = zone
        self.cluster_id = cluster_id
        self.name = name
        self.wait_for_completion = wait_for_completion
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegates_to = delegates_to

    def execute(self, context):
        if self.hook is None:
            self.hook = ContainerHook(gcp_conn_id=self.gcp_conn_id, delegates_to=self.delegates_to)
        self.hook.delete_gke_cluster(
            project_id=self.project_id,
            zone=self.zone,
            cluster_id=self.cluster_id,
            wait_for_completion=self.wait_for_completion,
            name=self.name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class ContainerDeleteNodePoolOperator(BaseOperator):
    template_fields = ()
    ui_color = ""

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        zone: str,
        cluster_id: str,
        node_pool_id: str,
        name: Optional[str] = None,
        wait_for_completion: Optional[bool] = True,
        project_id: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: Optional[str] = 'google_cloud_default',
        delegates_to: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.hook = None
        self.zone = zone
        self.cluster_id = cluster_id
        self.node_pool_id = node_pool_id
        self.name = name
        self.wait_for_completion = wait_for_completion
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegates_to = delegates_to

    def execute(self, context):
        if self.hook is None:
            self.hook = ContainerHook(gcp_conn_id=self.gcp_conn_id, delegates_to=self.delegates_to)
        self.hook.delete_node_pool(
            project_id=self.project_id,
            zone=self.zone,
            cluster_id=self.cluster_id,
            node_pool_id=self.node_pool_id,
            wait_for_completion=self.wait_for_completion,
            name=self.name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class ContainerSetNodePoolSizeOperator(BaseOperator):
    template_fields = ()
    ui_color = ""

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        zone: str,
        cluster_id: str,
        node_pool_id: str,
        node_count: int,
        wait_for_completion: Optional[bool] = True,
        name: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[str] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: Optional[str] = 'google_cloud_default',
        delegates_to: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.hook = None
        self.zone = zone
        self.cluster_id = cluster_id
        self.node_pool_id = node_pool_id
        self.node_count = node_count
        self.wait_for_completion = wait_for_completion
        self.name = name
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegates_to = delegates_to

    def execute(self, context):
        if self.hook is None:
            self.hook = ContainerHook(gcp_conn_id=self.gcp_conn_id, delegates_to=self.delegates_to)
        self.hook.set_node_pool_size(
            project_id=self.project_id,
            zone=self.zone,
            cluster_id=self.cluster_id,
            node_pool_id=self.node_pool_id,
            node_count=self.node_count,
            wait_for_completion=self.wait_for_completion,
            name=self.name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
