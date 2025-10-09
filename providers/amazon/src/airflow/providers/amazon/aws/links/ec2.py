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

from airflow.providers.amazon.aws.links.base_aws import BASE_AWS_CONSOLE_LINK, BaseAwsLink


class EC2InstanceLink(BaseAwsLink):
    """Helper class for constructing Amazon EC2 instance links."""

    name = "Instance"
    key = "_instance_id"
    format_str = (
        BASE_AWS_CONSOLE_LINK + "/ec2/home?region={region_name}#InstanceDetails:instanceId={instance_id}"
    )


class EC2InstanceDashboardLink(BaseAwsLink):
    """
    Helper class for constructing Amazon EC2 console links.

    This is useful for displaying the list of EC2 instances, rather
    than a single instance.
    """

    name = "EC2 Instances"
    key = "_instance_dashboard"
    format_str = BASE_AWS_CONSOLE_LINK + "/ec2/home?region={region_name}#Instances:instanceId=:{instance_ids}"

    @staticmethod
    def format_instance_id_filter(instance_ids: list[str]) -> str:
        return ",:".join(instance_ids)
