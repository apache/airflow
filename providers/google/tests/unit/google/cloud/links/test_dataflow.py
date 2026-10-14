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

from airflow.providers.google.cloud.links.dataflow import (
    DataflowJobLink,
    DataflowPipelineLink,
)

EXPECTED_DATAFLOW_JOB_LINK_NAME = "Dataflow Job"
EXPECTED_DATAFLOW_JOB_LINK_KEY = "dataflow_job_config"
EXPECTED_DATAFLOW_JOB_LINK_FORMAT_STR = "/dataflow/jobs/{region}/{job_id}?project={project_id}"
EXPECTED_DATAFLOW_PIPELINE_LINK_NAME = "Dataflow Pipeline"
EXPECTED_DATAFLOW_PIPELINE_LINK_KEY = "dataflow_pipeline_config"
EXPECTED_DATAFLOW_PIPELINE_LINK_FORMAT_STR = (
    "/dataflow/pipelines/{location}/{pipeline_name}?project={project_id}"
)


class TestDataflowJobLink:
    def test_class_attributes(self):
        assert DataflowJobLink.name == EXPECTED_DATAFLOW_JOB_LINK_NAME
        assert DataflowJobLink.key == EXPECTED_DATAFLOW_JOB_LINK_KEY
        assert DataflowJobLink.format_str == EXPECTED_DATAFLOW_JOB_LINK_FORMAT_STR


class TestDataflowPipelineLink:
    def test_class_attributes(self):
        assert DataflowPipelineLink.name == EXPECTED_DATAFLOW_PIPELINE_LINK_NAME
        assert DataflowPipelineLink.key == EXPECTED_DATAFLOW_PIPELINE_LINK_KEY
        assert DataflowPipelineLink.format_str == EXPECTED_DATAFLOW_PIPELINE_LINK_FORMAT_STR
