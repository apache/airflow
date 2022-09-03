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

from typing import Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.docker.credentials.base import BaseDockerCredentialHelper, DockerLoginCredentials


class AirflowConnectionDockerCredentialHelper(BaseDockerCredentialHelper):
    """Default helper for authentication in Docker Registry.
    Use Airflow Connection information.
    """

    def get_credentials(self) -> Optional[Sequence[DockerLoginCredentials]]:
        if not self.conn.host:
            raise AirflowException('No Docker URL provided.')
        if not self.conn.login:
            raise AirflowException('No username provided.')

        if self.conn.port:
            registry = f"{self.conn.host}:{self.conn.port}"
        else:
            registry = self.conn.host

        return [
            DockerLoginCredentials(
                username=self.conn.login,
                password=self.conn.password,
                registry=registry,
                email=self.email,
                reauth=self.reauth,
            )
        ]
