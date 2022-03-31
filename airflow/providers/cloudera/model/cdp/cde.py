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
"""Holds model objects related to CDE Environments."""

import re
from urllib.parse import urlparse


class VirtualCluster:
    """Represents a CDE Virtual cluster and hold various helper methods."""

    ACCESS_KEY_AUTH_ENDPOINT_PATH = "/gateway/cdptkn/knoxtoken/api/v1/token"

    def __init__(self, vcluster_endpoint: str) -> None:
        """
        Args:
            vcluster_endpoint: the endpoint of the Virtual Cluster corresponding to the
                               Job API URL
        """
        self.vcluster_endpoint = vcluster_endpoint
        auth_endpoint_url = urlparse(vcluster_endpoint)
        self.host = auth_endpoint_url.hostname

    def get_service_id(self) -> str:
        """
        Obtains cluster id from the virtual cluster endpoint.

        Returns:
            cluster id string in the form 'cluster-<cluster_id>'

        Raises:
            ValueError: When the given url is not in the expected format
        """
        pattern = re.compile("cde-[a-zA-Z0-9]*")
        try:
            first_match = pattern.findall(self.vcluster_endpoint)[0]
            return re.sub("^cde-", "cluster-", first_match, 1)
        except IndexError as err:
            raise ValueError(f"Cluster ID not found in {self.vcluster_endpoint}") from err

    def get_auth_endpoint(self) -> str:
        """
        Derive the authentication endpoint from the virtual cluster cluster endpoint

        Returns:
            Endpoint of the authentication service

        Raises:
            ValueError of the input has incorrect form
        """
        auth_endpoint = re.sub("^https://[a-zA-Z0-9]*", "https://service", self.vcluster_endpoint, 1)

        if auth_endpoint == self.vcluster_endpoint:
            raise ValueError(
                f"Invalid vcluster endpoint given: {self.vcluster_endpoint}",
            )

        auth_endpoint_url = urlparse(auth_endpoint)
        auth_endpoint_url = auth_endpoint_url._replace(path=self.ACCESS_KEY_AUTH_ENDPOINT_PATH)
        return auth_endpoint_url.geturl()
