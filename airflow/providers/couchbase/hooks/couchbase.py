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
"""Hook for Couchbase."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any, overload, Optional,Union

import couchbase
from couchbase.cluster import Cluster
import couchbase.collection
from couchbase.options import ClusterOptions, Compression, IpProtocol, KnownConfigProfiles, TLSVerifyMode

from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from types import TracebackType


class Config(dict):
    @overload
    def __init__(
        self,
        profile: Optional[KnownConfigProfiles] = None,
        # timeout_options
        bootstrap_timeout: Optional[timedelta] = None,
        resolve_timeout: Optional[timedelta] = None,
        connect_timeout: Optional[timedelta] = None,
        kv_timeout: Optional[timedelta] = None,
        kv_durable_timeout: Optional[timedelta] = None,
        views_timeout: Optional[timedelta] = None,
        query_timeout: Optional[timedelta] = None,
        analytics_timeout: Optional[timedelta] = None,
        search_timeout: Optional[timedelta] = None,
        management_timeout: Optional[timedelta] = None,
        dns_srv_timeout: Optional[timedelta] = None,
        idle_http_connection_timeout: Optional[timedelta] = None,
        config_idle_redial_timeout: Optional[timedelta] = None,
        config_total_timeout: Optional[timedelta] = None,
        # timeout_options
        # tracing_options
        tracing_threshold_kv: Optional[timedelta] = None,
        tracing_threshold_view: Optional[timedelta] = None,
        tracing_threshold_query: Optional[timedelta] = None,
        tracing_threshold_search: Optional[timedelta] = None,
        tracing_threshold_analytics: Optional[timedelta] = None,
        tracing_threshold_eventing: Optional[timedelta] = None,
        tracing_threshold_management: Optional[timedelta] = None,
        tracing_threshold_queue_size: Optional[int] = None,
        tracing_threshold_queue_flush_interval: Optional[timedelta] = None,
        tracing_orphaned_queue_size: Optional[int] = None,
        tracing_orphaned_queue_flush_interval: Optional[timedelta] = None,
        # tracing_options
        enable_tls: Optional[bool] = None,
        enable_mutation_tokens: Optional[bool] = None,
        enable_tcp_keep_alive: Optional[bool] = None,
        ip_protocol: Optional[Union[IpProtocol, str]] = None,
        enable_dns_srv: Optional[bool] = None,
        show_queries: Optional[bool] = None,
        enable_unordered_execution: Optional[bool] = None,
        enable_clustermap_notification: Optional[bool] = None,
        enable_compression: Optional[bool] = None,
        enable_tracing: Optional[bool] = None,
        enable_metrics: Optional[bool] = None,
        network: Optional[str] = None,
        tls_verify: Optional[Union[TLSVerifyMode, str]] = None,
        tcp_keep_alive_interval: Optional[timedelta] = None,
        config_poll_interval: Optional[timedelta] = None,
        config_poll_floor: Optional[timedelta] = None,
        max_http_connections: Optional[int] = None,
        user_agent_extra: Optional[str] = None,
        logging_meter_emit_interval: Optional[timedelta] = None,
        log_redaction: Optional[bool] = None,
        compression: Optional[Compression] = None,
        compression_min_size: Optional[int] = None,
        compression_min_ratio: Optional[float] = None,
        dns_nameserver: Optional[str] = None,
        dns_port: Optional[int] = None,
        disable_mozilla_ca_certificates: Optional[bool] = None,
        dump_configuration: Optional[bool] = None,
    ):
        """Config instance."""

    @overload
    def __init__(self, **kwargs):
        """Config instance."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class CouchbaseHook(BaseHook):
    """
    Couchbase Connection Class

    Documentation on establishing a connection can be found here:
    https://docs.couchbase.com/python-sdk/current/hello-world/start-using-sdk.html

    When setting up the Couchbase connection, you can specify the certificate path in the `extra` field of your connection configuration.
    Example configuration:
    
    ```json
    {
        "cert_path": "/path/to/custom/ca-cert",
        "key_path": "/path/to/key-file",
        "trust_store_path": "/path/to/cert-file"
    }
    ```

    - `cert_path`: This is a common field used for both password and certificate-based authenticators.
    - In the case of **password authentication**, only the `cert_path` field is considered.
    - For **certificate authentication**, you must specify `cert_path`, `key_path`, and `trust_store_path`.

    Make sure to provide valid paths according to your environment.
    """

    conn_name_attr = "couchbase_conn_id"
    default_conn_name = "couchbase_default"
    conn_type = "couchbase"
    hook_name = "Couchbase"
    default_config = Config()

    def __init__(self, conn_id: str = default_conn_name, config: Config = default_config, *args, **kwargs) -> None:
        super().__init__()
        self.couchbase_conn_id = conn_id
        self.cluster_config = config
        self.cluster: Cluster | None = None

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "uri"],
            "relabeling": {
                "host": "connection",
                "login": "username",
            },
            "placeholders": {
                "login": "Username to use for authentication",
                "password": "Password to use for authentication",
                "host": "Couchbase connection string",
            },
        }       

    def __enter__(self):
        """Return the object when a context manager is created."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close couchbase cluster when exiting the context manager."""
        if self.cluster is not None:
            self.cluster.close()
            self.cluster = None

    def get_conn(self) -> Cluster:
        """Fetch Couchbase Cluster."""
        if self.cluster is not None:
            return self.cluster

        connection = self.get_connection(self.couchbase_conn_id)
        connection_str = connection.host
        username = connection.login
        password = connection.password
        cert_path = connection.extra_dejson.get("cert_path")
        key_path = connection.extra_dejson.get("key_path")
        trust_store_path = connection.extra_dejson.get("trust_store_path")

        auth: couchbase.auth.Authenticator
        if not username and not password:
            auth = couchbase.auth.CertificateAuthenticator(cert_path, key_path, trust_store_path)
        else: 
            auth = couchbase.auth.PasswordAuthenticator(username, password, cert_path)

        config = {**self.cluster_config}
        profile = config.get("profile")
        if profile:
            del config["profile"]
        options = ClusterOptions(authenticator=auth, **config)
        if profile:
            options.apply_profile(profile)
        self.cluster = Cluster(connection_str, options)
        return self.cluster
    
    def get_scope(
        self, bucket: str, scope: str
    ) -> couchbase.collection.Scope: 
        """
        Fetch a couchbase scope object for querying.
        """
        cluster = self.get_conn()
        return cluster.bucket(bucket).scope(scope)

    def get_collection(
        self, bucket: str, scope: str,collection: str
    ) -> couchbase.collection.Collection: 
        """
        Fetch a couchbase collection object for querying.
        """
        cluster = self.get_conn()
        return cluster.bucket(bucket).scope(scope).collection(collection)