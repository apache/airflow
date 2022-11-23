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
"""Holds connections for the Cloudera Products"""

from __future__ import annotations

import json
import logging
from json.decoder import JSONDecodeError
from urllib.parse import urlparse

from airflow.models.connection import Connection
from airflow.utils.session import provide_session

LOG = logging.getLogger(__name__)


class CdeConnection(Connection):
    """Connection details to the Cloudera Data Engineering product"""

    CDE_API_PREFIX = "/api/v1"

    def __init__(  # pylint: disable=too-many-arguments
        self,
        connection_id: str,
        scheme: str,
        host: str,
        api_base_route: str,
        access_key: str,
        private_key: str,
        port: int | None = None,
        cache_dir: str | None = None,
        ca_cert_path: str | None = None,
        proxy: str | None = None,
        cdp_endpoint: str | None = None,
        altus_iam_endpoint: str | None = None,
        insecure: bool = False,
        region: str | None = None,
    ) -> None:
        super().__init__(
            conn_id=connection_id,
            host=host,
            login=access_key,
            password=private_key,
            port=port,
        )
        self.conn_type = "cloudera_data_engineering"
        self.scheme = scheme
        self.api_base_route = api_base_route
        self.cache_dir = cache_dir
        self.ca_cert_path = ca_cert_path
        self.proxy = proxy
        self.cdp_endpoint = cdp_endpoint
        self.altus_iam_endpoint = altus_iam_endpoint
        self.insecure = insecure
        self.region = region

    def is_external(self) -> bool:
        """Checks if connection is external. External connections
        are typically cross-services connections or connection defined
        in an external Airflow instance.

        Returns:
            True of connection is external, false otherwise
        """
        return not self.is_internal()

    def is_internal(self) -> bool:
        """Checks if connection is internal. Internal connections
        are only meant to be used within a CDE service and are managed
        automatically by the Virtual Cluster.

        Returns:
            True of connection is internal, false otherwise
        """
        return self.__internal_connection(self.host)

    def get_vcluster_jobs_api_url(self) -> str:
        """Constructs the jobs api url from the elements defined in the connection.

        Returns:
            vcluster_jobs_api_url: the jobs api url
        """
        vcluster_jobs_api_url = f"{self.scheme}://{self.host}"
        if self.port:
            vcluster_jobs_api_url += ":" + str(self.port)
        vcluster_jobs_api_url += self.api_base_route
        return vcluster_jobs_api_url

    @provide_session
    def save_region(self, region: str, session=None):
        """Save region, so that any subsequent calls would not need to infer it again."""
        self.region = region
        connection = session.query(Connection).filter_by(conn_id=self.conn_id).one_or_none()
        if not connection:
            LOG.warning(
                "Can not save region. The connection with connection_id: %s was not found", self.conn_id
            )
            return
        extra = json.loads(connection.extra) if connection.extra else {}
        extra["region"] = region
        connection.set_extra(json.dumps(extra))
        session.add(connection)
        session.commit()

    @property
    def access_key(self) -> str:
        """CDP Access key

        Returns:
            the access key associated to the connection
        """
        return self.login

    @property
    def private_key(self) -> str:
        """CDP Private key

        Returns:
            the private key associated to the connection
        """
        # Relies on Airflow Connection password getter,
        # so that the password is not stored in clear in the memory
        return self.password

    @classmethod
    def __internal_connection(cls, hostname: str) -> bool:
        return hostname.endswith(".svc") or hostname.endswith(".svc.cluster.local")

    @classmethod
    def from_airflow_connection(cls, conn: Connection) -> CdeConnection:
        """Factory method for constructing a CDE connection from an Airflow Connection.

        Args:
            conn: an Airflow Connection instance

        Returns:
            A new CDE connection with the parameters derived from the Airflow connection
        """
        try:
            if conn.extra:
                extra = json.loads(conn.extra)
            else:
                extra = {}
        except JSONDecodeError as err:
            raise ValueError(f"Invalid extra property: {repr(err)}") from err
        if conn.host and "://" in conn.host:
            conn_uri = conn.host
        else:
            conn_uri = conn.get_uri()
        connection_url = urlparse(conn_uri)

        # Internal endpoints have base prefix
        api_base_route = (
            cls.CDE_API_PREFIX if cls.__internal_connection(connection_url.hostname) else connection_url.path
        )

        return cls(
            conn.conn_id,
            connection_url.scheme,
            connection_url.hostname,
            api_base_route,
            conn.login,
            conn.password,
            port=conn.port,
            cache_dir=extra.get("cache_dir"),
            ca_cert_path=extra.get("ca_cert_path"),
            proxy=extra.get("proxy"),
            cdp_endpoint=extra.get("cdp_endpoint"),
            altus_iam_endpoint=extra.get("altus_iam_endpoint"),
            insecure=extra.get("insecure", False),
            region=extra.get("region"),
        )

    def __repr__(self) -> str:
        return repr(self.__dict__)
