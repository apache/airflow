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

from airflow.providers.google.cloud.links.bigquery_dts import BigQueryDataTransferConfigLink

EXPECTED_BIG_QUERY_DATA_TRANSFER_CONFIG_LINK_NAME = "BigQuery Data Transfer Config"
EXPECTED_BIG_QUERY_DATA_TRANSFER_CONFIG_LINK_KEY = "bigquery_dts_config"
EXPECTED_BIG_QUERY_DATA_TRANSFER_CONFIG_LINK_FORMAT_STR = (
    "/bigquery/transfers/locations/{region}/configs/{config_id}/runs?project={project_id}"
)


class TestBigQueryDataTransferConfigLink:
    def test_class_attributes(self):
        assert BigQueryDataTransferConfigLink.name == EXPECTED_BIG_QUERY_DATA_TRANSFER_CONFIG_LINK_NAME
        assert BigQueryDataTransferConfigLink.key == EXPECTED_BIG_QUERY_DATA_TRANSFER_CONFIG_LINK_KEY
        assert (
            BigQueryDataTransferConfigLink.format_str
            == EXPECTED_BIG_QUERY_DATA_TRANSFER_CONFIG_LINK_FORMAT_STR
        )
