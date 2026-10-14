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
from airflow.providers.google.cloud.links.cloud_functions import (
    CloudFunctionsDetailsLink,
    CloudFunctionsListLink,
)

EXPECTED_CLOUD_FUNCTIONS_DETAILS_LINK_NAME = "Cloud Functions Details"
EXPECTED_CLOUD_FUNCTIONS_DETAILS_LINK_KEY = "cloud_functions_details"
EXPECTED_CLOUD_FUNCTIONS_DETAILS_LINK_FORMAT_STR = (
    BASE_LINK + "/functions/details/{location}/{function_name}?project={project_id}"
)
EXPECTED_CLOUD_FUNCTIONS_LIST_LINK_NAME = "Cloud Functions List"
EXPECTED_CLOUD_FUNCTIONS_LIST_LINK_KEY = "cloud_functions_list"
EXPECTED_CLOUD_FUNCTIONS_LIST_LINK_FORMAT_STR = BASE_LINK + "/functions/list?project={project_id}"


class TestCloudFunctionsDetailsLink:
    def test_class_attributes(self):
        assert CloudFunctionsDetailsLink.name == EXPECTED_CLOUD_FUNCTIONS_DETAILS_LINK_NAME
        assert CloudFunctionsDetailsLink.key == EXPECTED_CLOUD_FUNCTIONS_DETAILS_LINK_KEY
        assert CloudFunctionsDetailsLink.format_str == EXPECTED_CLOUD_FUNCTIONS_DETAILS_LINK_FORMAT_STR


class TestCloudFunctionsListLink:
    def test_class_attributes(self):
        assert CloudFunctionsListLink.name == EXPECTED_CLOUD_FUNCTIONS_LIST_LINK_NAME
        assert CloudFunctionsListLink.key == EXPECTED_CLOUD_FUNCTIONS_LIST_LINK_KEY
        assert CloudFunctionsListLink.format_str == EXPECTED_CLOUD_FUNCTIONS_LIST_LINK_FORMAT_STR
