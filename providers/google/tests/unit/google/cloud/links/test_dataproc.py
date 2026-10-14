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
from __future__ import annotations

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.dataproc import (
    DataprocBatchesListLink,
    DataprocBatchLink,
    DataprocClusterLink,
    DataprocJobLink,
    DataprocLink,
    DataprocListLink,
    DataprocWorkflowLink,
    DataprocWorkflowTemplateLink,
)

EXPECTED_DATAPROC_LINK_NAME = "Dataproc resource"
EXPECTED_DATAPROC_LINK_KEY = "conf"
EXPECTED_DATAPROC_LIST_LINK_NAME = "Dataproc resources"
EXPECTED_DATAPROC_LIST_LINK_KEY = "list_conf"
EXPECTED_DATAPROC_CLUSTER_LINK_NAME = "Dataproc Cluster"
EXPECTED_DATAPROC_CLUSTER_LINK_KEY = "dataproc_cluster"
EXPECTED_DATAPROC_CLUSTER_LINK_FORMAT_STR = (
    BASE_LINK + "/dataproc/clusters/{cluster_id}/monitoring?region={region}&project={project_id}"
)
EXPECTED_DATAPROC_JOB_LINK_NAME = "Dataproc Job"
EXPECTED_DATAPROC_JOB_LINK_KEY = "dataproc_job"
EXPECTED_DATAPROC_JOB_LINK_FORMAT_STR = (
    BASE_LINK + "/dataproc/jobs/{job_id}?region={region}&project={project_id}"
)
EXPECTED_DATAPROC_WORKFLOW_LINK_NAME = "Dataproc Workflow"
EXPECTED_DATAPROC_WORKFLOW_LINK_KEY = "dataproc_workflow"
EXPECTED_DATAPROC_WORKFLOW_LINK_FORMAT_STR = (
    BASE_LINK + "/dataproc/workflows/instances/{region}/{workflow_id}?project={project_id}"
)
EXPECTED_DATAPROC_WORKFLOW_TEMPLATE_LINK_NAME = "Dataproc Workflow Template"
EXPECTED_DATAPROC_WORKFLOW_TEMPLATE_LINK_KEY = "dataproc_workflow_template"
EXPECTED_DATAPROC_WORKFLOW_TEMPLATE_LINK_FORMAT_STR = (
    BASE_LINK + "/dataproc/workflows/templates/{region}/{workflow_template_id}?project={project_id}"
)
EXPECTED_DATAPROC_BATCH_LINK_NAME = "Dataproc Batch"
EXPECTED_DATAPROC_BATCH_LINK_KEY = "dataproc_batch"
EXPECTED_DATAPROC_BATCH_LINK_FORMAT_STR = (
    BASE_LINK + "/dataproc/batches/{region}/{batch_id}/monitoring?project={project_id}"
)
EXPECTED_DATAPROC_BATCHES_LIST_LINK_NAME = "Dataproc Batches List"
EXPECTED_DATAPROC_BATCHES_LIST_LINK_KEY = "dataproc_batches_list"
EXPECTED_DATAPROC_BATCHES_LIST_LINK_FORMAT_STR = BASE_LINK + "/dataproc/batches?project={project_id}"


class TestDataprocLink:
    def test_class_attributes(self):
        assert DataprocLink.name == EXPECTED_DATAPROC_LINK_NAME
        assert DataprocLink.key == EXPECTED_DATAPROC_LINK_KEY


class TestDataprocListLink:
    def test_class_attributes(self):
        assert DataprocListLink.name == EXPECTED_DATAPROC_LIST_LINK_NAME
        assert DataprocListLink.key == EXPECTED_DATAPROC_LIST_LINK_KEY


class TestDataprocClusterLink:
    def test_class_attributes(self):
        assert DataprocClusterLink.name == EXPECTED_DATAPROC_CLUSTER_LINK_NAME
        assert DataprocClusterLink.key == EXPECTED_DATAPROC_CLUSTER_LINK_KEY
        assert DataprocClusterLink.format_str == EXPECTED_DATAPROC_CLUSTER_LINK_FORMAT_STR


class TestDataprocJobLink:
    def test_class_attributes(self):
        assert DataprocJobLink.name == EXPECTED_DATAPROC_JOB_LINK_NAME
        assert DataprocJobLink.key == EXPECTED_DATAPROC_JOB_LINK_KEY
        assert DataprocJobLink.format_str == EXPECTED_DATAPROC_JOB_LINK_FORMAT_STR


class TestDataprocWorkflowLink:
    def test_class_attributes(self):
        assert DataprocWorkflowLink.name == EXPECTED_DATAPROC_WORKFLOW_LINK_NAME
        assert DataprocWorkflowLink.key == EXPECTED_DATAPROC_WORKFLOW_LINK_KEY
        assert DataprocWorkflowLink.format_str == EXPECTED_DATAPROC_WORKFLOW_LINK_FORMAT_STR


class TestDataprocWorkflowTemplateLink:
    def test_class_attributes(self):
        assert DataprocWorkflowTemplateLink.name == EXPECTED_DATAPROC_WORKFLOW_TEMPLATE_LINK_NAME
        assert DataprocWorkflowTemplateLink.key == EXPECTED_DATAPROC_WORKFLOW_TEMPLATE_LINK_KEY
        assert DataprocWorkflowTemplateLink.format_str == EXPECTED_DATAPROC_WORKFLOW_TEMPLATE_LINK_FORMAT_STR


class TestDataprocBatchLink:
    def test_class_attributes(self):
        assert DataprocBatchLink.name == EXPECTED_DATAPROC_BATCH_LINK_NAME
        assert DataprocBatchLink.key == EXPECTED_DATAPROC_BATCH_LINK_KEY
        assert DataprocBatchLink.format_str == EXPECTED_DATAPROC_BATCH_LINK_FORMAT_STR


class TestDataprocBatchesListLink:
    def test_class_attributes(self):
        assert DataprocBatchesListLink.name == EXPECTED_DATAPROC_BATCHES_LIST_LINK_NAME
        assert DataprocBatchesListLink.key == EXPECTED_DATAPROC_BATCHES_LIST_LINK_KEY
        assert DataprocBatchesListLink.format_str == EXPECTED_DATAPROC_BATCHES_LIST_LINK_FORMAT_STR
