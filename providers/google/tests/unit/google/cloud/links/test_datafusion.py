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
from airflow.providers.google.cloud.links.datafusion import (
    DataFusionInstanceLink,
    DataFusionPipelineLink,
    DataFusionPipelinesLink,
)

EXPECTED_DATA_FUSION_INSTANCE_LINK_NAME = "Data Fusion Instance"
EXPECTED_DATA_FUSION_INSTANCE_LINK_KEY = "instance_conf"
EXPECTED_DATA_FUSION_INSTANCE_LINK_FORMAT_STR = (
    BASE_LINK + "/data-fusion/locations/{region}/instances/{instance_name}?project={project_id}"
)
EXPECTED_DATA_FUSION_PIPELINE_LINK_NAME = "Data Fusion Pipeline"
EXPECTED_DATA_FUSION_PIPELINE_LINK_KEY = "pipeline_conf"
EXPECTED_DATA_FUSION_PIPELINE_LINK_FORMAT_STR = "{uri}/pipelines/ns/{namespace}/view/{pipeline_name}"
EXPECTED_DATA_FUSION_PIPELINES_LINK_NAME = "Data Fusion Pipelines List"
EXPECTED_DATA_FUSION_PIPELINES_LINK_KEY = "pipelines_conf"
EXPECTED_DATA_FUSION_PIPELINES_LINK_FORMAT_STR = "{uri}/cdap/ns/{namespace}/pipelines"


class TestDataFusionInstanceLink:
    def test_class_attributes(self):
        assert DataFusionInstanceLink.name == EXPECTED_DATA_FUSION_INSTANCE_LINK_NAME
        assert DataFusionInstanceLink.key == EXPECTED_DATA_FUSION_INSTANCE_LINK_KEY
        assert DataFusionInstanceLink.format_str == EXPECTED_DATA_FUSION_INSTANCE_LINK_FORMAT_STR


class TestDataFusionPipelineLink:
    def test_class_attributes(self):
        assert DataFusionPipelineLink.name == EXPECTED_DATA_FUSION_PIPELINE_LINK_NAME
        assert DataFusionPipelineLink.key == EXPECTED_DATA_FUSION_PIPELINE_LINK_KEY
        assert DataFusionPipelineLink.format_str == EXPECTED_DATA_FUSION_PIPELINE_LINK_FORMAT_STR


class TestDataFusionPipelinesLink:
    def test_class_attributes(self):
        assert DataFusionPipelinesLink.name == EXPECTED_DATA_FUSION_PIPELINES_LINK_NAME
        assert DataFusionPipelinesLink.key == EXPECTED_DATA_FUSION_PIPELINES_LINK_KEY
        assert DataFusionPipelinesLink.format_str == EXPECTED_DATA_FUSION_PIPELINES_LINK_FORMAT_STR
