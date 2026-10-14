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
from airflow.providers.google.cloud.links.compute import (
    ComputeInstanceDetailsLink,
    ComputeInstanceGroupManagerDetailsLink,
    ComputeInstanceTemplateDetailsLink,
)

EXPECTED_COMPUTE_INSTANCE_DETAILS_LINK_NAME = "Compute Instance details"
EXPECTED_COMPUTE_INSTANCE_DETAILS_LINK_KEY = "compute_instance_details"
EXPECTED_COMPUTE_INSTANCE_DETAILS_LINK_FORMAT_STR = (
    BASE_LINK + "/compute/instancesDetail/zones/{location_id}/instances/{resource_id}?project={project_id}"
)
EXPECTED_COMPUTE_INSTANCE_TEMPLATE_DETAILS_LINK_NAME = "Compute Instance Template details"
EXPECTED_COMPUTE_INSTANCE_TEMPLATE_DETAILS_LINK_KEY = "compute_instance_template_details"
EXPECTED_COMPUTE_INSTANCE_TEMPLATE_DETAILS_LINK_FORMAT_STR = (
    BASE_LINK + "/compute/instanceTemplates/details/{resource_id}?project={project_id}"
)
EXPECTED_COMPUTE_INSTANCE_GROUP_MANAGER_DETAILS_LINK_NAME = "Compute Instance Group Manager"
EXPECTED_COMPUTE_INSTANCE_GROUP_MANAGER_DETAILS_LINK_KEY = "compute_instance_group_manager_details"
EXPECTED_COMPUTE_INSTANCE_GROUP_MANAGER_DETAILS_LINK_FORMAT_STR = (
    BASE_LINK + "/compute/instanceGroups/details/{location_id}/{resource_id}?project={project_id}"
)


class TestComputeInstanceDetailsLink:
    def test_class_attributes(self):
        assert ComputeInstanceDetailsLink.name == EXPECTED_COMPUTE_INSTANCE_DETAILS_LINK_NAME
        assert ComputeInstanceDetailsLink.key == EXPECTED_COMPUTE_INSTANCE_DETAILS_LINK_KEY
        assert ComputeInstanceDetailsLink.format_str == EXPECTED_COMPUTE_INSTANCE_DETAILS_LINK_FORMAT_STR


class TestComputeInstanceTemplateDetailsLink:
    def test_class_attributes(self):
        assert ComputeInstanceTemplateDetailsLink.name == EXPECTED_COMPUTE_INSTANCE_TEMPLATE_DETAILS_LINK_NAME
        assert ComputeInstanceTemplateDetailsLink.key == EXPECTED_COMPUTE_INSTANCE_TEMPLATE_DETAILS_LINK_KEY
        assert (
            ComputeInstanceTemplateDetailsLink.format_str
            == EXPECTED_COMPUTE_INSTANCE_TEMPLATE_DETAILS_LINK_FORMAT_STR
        )


class TestComputeInstanceGroupManagerDetailsLink:
    def test_class_attributes(self):
        assert (
            ComputeInstanceGroupManagerDetailsLink.name
            == EXPECTED_COMPUTE_INSTANCE_GROUP_MANAGER_DETAILS_LINK_NAME
        )
        assert (
            ComputeInstanceGroupManagerDetailsLink.key
            == EXPECTED_COMPUTE_INSTANCE_GROUP_MANAGER_DETAILS_LINK_KEY
        )
        assert (
            ComputeInstanceGroupManagerDetailsLink.format_str
            == EXPECTED_COMPUTE_INSTANCE_GROUP_MANAGER_DETAILS_LINK_FORMAT_STR
        )
