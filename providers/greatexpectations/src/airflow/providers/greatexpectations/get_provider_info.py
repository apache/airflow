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
"""Provider info for Great Expectations provider."""

from airflow.providers.greatexpectations.common.constants import VERSION


def get_provider_info():
    """Return provider information for Great Expectations."""
    return {
        "package-name": "apache-airflow-providers-greatexpectations",
        "name": "Great Expectations",
        "description": "An Apache Airflow provider for Great Expectations.",
        "versions": [VERSION],
        "hooks": [
            {
                "integration-name": "Great Expectations Cloud",
                "python-modules": ["airflow.providers.greatexpectations.hooks.gx_cloud"],
            }
        ],
        "operators": [
            {
                "integration-name": "Great Expectations",
                "python-modules": [
                    "airflow.providers.greatexpectations.operators.validate_checkpoint",
                    "airflow.providers.greatexpectations.operators.validate_dataframe",
                    "airflow.providers.greatexpectations.operators.validate_batch",
                ],
            }
        ],
        "connection-types": [
            {
                "connection-type": "gx_cloud",
                "hook-class-name": "airflow.providers.greatexpectations.hooks.gx_cloud.GXCloudHook",
            }
        ],
    } 