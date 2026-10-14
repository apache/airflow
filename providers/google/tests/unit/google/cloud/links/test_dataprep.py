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

from airflow.providers.google.cloud.links.dataprep import (
    DataprepFlowLink,
    DataprepJobGroupLink,
)

EXPECTED_DATAPREP_FLOW_LINK_NAME = "Flow details page"
EXPECTED_DATAPREP_FLOW_LINK_KEY = "dataprep_flow_page"
EXPECTED_DATAPREP_FLOW_LINK_FORMAT_STR = "https://clouddataprep.com/flows/{flow_id}?projectId={project_id}"
EXPECTED_DATAPREP_JOB_GROUP_LINK_NAME = "Job group details page"
EXPECTED_DATAPREP_JOB_GROUP_LINK_KEY = "dataprep_job_group_page"
EXPECTED_DATAPREP_JOB_GROUP_LINK_FORMAT_STR = (
    "https://clouddataprep.com/jobs/{job_group_id}?projectId={project_id}"
)


class TestDataprepFlowLink:
    def test_class_attributes(self):
        assert DataprepFlowLink.name == EXPECTED_DATAPREP_FLOW_LINK_NAME
        assert DataprepFlowLink.key == EXPECTED_DATAPREP_FLOW_LINK_KEY
        assert DataprepFlowLink.format_str == EXPECTED_DATAPREP_FLOW_LINK_FORMAT_STR


class TestDataprepJobGroupLink:
    def test_class_attributes(self):
        assert DataprepJobGroupLink.name == EXPECTED_DATAPREP_JOB_GROUP_LINK_NAME
        assert DataprepJobGroupLink.key == EXPECTED_DATAPREP_JOB_GROUP_LINK_KEY
        assert DataprepJobGroupLink.format_str == EXPECTED_DATAPREP_JOB_GROUP_LINK_FORMAT_STR
