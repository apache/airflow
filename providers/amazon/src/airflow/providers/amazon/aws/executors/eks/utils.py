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
"""Configuration keys and defaults used by the EKS executor."""

from __future__ import annotations

from airflow.providers.amazon.aws.executors.utils.base_config_keys import BaseConfigKeys

CONFIG_GROUP_NAME = "aws_eks_executor"

CONFIG_DEFAULTS = {
    "conn_id": "aws_default",
    "check_health_on_startup": "True",
}


class AllEksConfigKeys(BaseConfigKeys):
    """All keys loaded into the config which are related to the EKS Executor."""

    AWS_CONN_ID = "conn_id"
    CHECK_HEALTH_ON_STARTUP = "check_health_on_startup"
    CLUSTER_NAME = "cluster_name"
    REGION_NAME = "region_name"
