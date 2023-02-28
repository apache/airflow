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

import jenkins

from airflow.hooks.base import BaseHook
from airflow.utils.strings import to_boolean


class JenkinsHook(BaseHook):
    """Hook to manage connection to jenkins server"""

    conn_name_attr = "conn_id"
    default_conn_name = "jenkins_default"
    conn_type = "jenkins"
    hook_name = "Jenkins"

    def __init__(self, conn_id: str = default_conn_name) -> None:
        super().__init__()
        connection = self.get_connection(conn_id)
        self.connection = connection
        connection_prefix = "http"
        # connection.extra contains info about using https (true) or http (false)
        if to_boolean(connection.extra):
            connection_prefix = "https"
        url = f"{connection_prefix}://{connection.host}:{connection.port}/{connection.schema}"
        self.log.info("Trying to connect to %s", url)
        self.jenkins_server = jenkins.Jenkins(url, connection.login, connection.password)

    def get_jenkins_server(self) -> jenkins.Jenkins:
        """Get jenkins server"""
        return self.jenkins_server

    def get_latest_build_number(self, job_name) -> int:
        self.log.info("Build number not specified, getting latest build info from Jenkins")
        job_info = self.jenkins_server.get_job_info(job_name)
        return job_info["lastBuild"]["number"]

    def get_build_result(self, job_name: str, build_number) -> str:
        build_info = self.jenkins_server.get_build_info(job_name, build_number)
        return build_info["result"]

    def get_build_building_state(self, job_name: str, build_number: int | None) -> bool:
        if not build_number:
            build_number_to_check = self.get_latest_build_number(job_name)
        else:
            build_number_to_check = build_number

        self.log.info("Getting build info for %s build number: #%s", job_name, build_number_to_check)
        build_info = self.jenkins_server.get_build_info(job_name, build_number_to_check)
        building = build_info["building"]
        return building
