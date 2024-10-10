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

from unittest import mock


import couchbase.auth
import couchbase.bucket
import couchbase.cluster
import couchbase.collection
import couchbase.scope
import pytest

from airflow.models import Connection
from airflow.providers.couchbase.hooks.couchbase import CouchbaseHook, Config, ClusterOptions
import couchbase

pytestmark = pytest.mark.db_test


class TestCouchbaseHook:
    @mock.patch("airflow.providers.couchbase.hooks.couchbase.Cluster")
    def test_get_conn(self, mock_cluster):
        hook = CouchbaseHook(conn_id="couchbase_default")
        assert hook.cluster is None
        assert hook.cluster_config is CouchbaseHook.default_config, "cluster_config initialised as ClusterOptions()."
        assert hook.get_conn() is hook.get_conn(), "Connection initialized only if None."

    @mock.patch("airflow.providers.couchbase.hooks.couchbase.Cluster")
    @mock.patch(
        "airflow.providers.couchbase.hooks.couchbase.CouchbaseHook.get_connection",
        return_value=Connection(
            login="user",
            password="password",
            host="remote_host",
            extra="""{
                        "cert_path": "/path/to/custom/ca-cert"
                    }""",
        ),
    )
    def test_get_conn_with_password_auth_extra_config(self, mock_get_connection, mock_cluster):
        connection = mock_get_connection.return_value
        hook = CouchbaseHook()

        hook.get_conn()
        mock_cluster.assert_called_once_with(
            connection.host,
            ClusterOptions(
                authenticator=couchbase.auth.PasswordAuthenticator(
                    username=connection.login,
                    password=connection.password,
                    cert_path= connection.extra_dejson.get("cert_path"),
                )
            ),
        )

    @mock.patch("airflow.providers.couchbase.hooks.couchbase.Cluster")
    @mock.patch(
        "airflow.providers.couchbase.hooks.couchbase.CouchbaseHook.get_connection",
        return_value=Connection(
            host="remote_host",
            extra="""{
                        "cert_path": "/path/to/custom/ca-cert",
                        "key_path": "/path/to/key-file",
                        "trust_store_path": "/path/to/cert-file"
                    }""",
        ),
    )
    def test_get_conn_with_certificate_auth_extra_config(self, mock_get_connection, mock_cluster):
        connection = mock_get_connection.return_value
        hook = CouchbaseHook()
        hook.get_conn()
        mock_cluster.assert_called_once_with(
            connection.host,
            ClusterOptions(
                authenticator=couchbase.auth.CertificateAuthenticator(
                    cert_path=connection.extra_dejson.get("cert_path"),
                    key_path=connection.extra_dejson.get("key_path"),
                    trust_store_path=connection.extra_dejson.get("trust_store_path"),
                )
            ),
        )

    @mock.patch("airflow.providers.couchbase.hooks.couchbase.Cluster")
    def test_get_conn_password_stays_default(self, mock_cluster):
        hook = CouchbaseHook(redis_conn_id="couchbase_default")
        hook.get_conn()
        assert hook.cluster is not None
        mock_cluster.assert_called_once_with(
            "localhost",
            ClusterOptions(
                authenticator=couchbase.auth.PasswordAuthenticator(
                    username="username",
                    password="password",
                )
            ),
        )

    @mock.patch("airflow.providers.couchbase.hooks.couchbase.Cluster")
    def test_get_scope(self, mock_cluster: mock.MagicMock):
        # Mock the cluster object
        mock_cluster_instance = mock.Mock(spec=couchbase.cluster.Cluster)
        mock_cluster.return_value = mock_cluster_instance

        # Mock bucket and scope behavior
        mock_bucket = mock.Mock(spec=couchbase.bucket.Bucket)
        mock_cluster_instance.bucket.return_value = mock_bucket
        mock_scope = mock.Mock(spec=couchbase.scope.Scope)
        mock_bucket.scope.return_value = mock_scope

        hook = CouchbaseHook()
        scope = hook.get_scope(bucket="bucket", scope="scope")
        assert hook.cluster is not None
        mock_cluster.assert_called_once_with(
            "localhost",
            ClusterOptions(
                authenticator=couchbase.auth.PasswordAuthenticator(
                    username="username",
                    password="password",
                )
            ),
        )    
        mock_cluster_instance.bucket.assert_called_once_with("bucket")
        mock_bucket.scope.assert_called_once_with("scope")
        assert isinstance(scope, couchbase.scope.Scope), "collection is not of type Collection"

    @mock.patch("airflow.providers.couchbase.hooks.couchbase.Cluster")
    def test_get_collection(self, mock_cluster: mock.MagicMock):
        # Mock the cluster object
        mock_cluster_instance = mock.Mock(spec=couchbase.cluster.Cluster)
        mock_cluster.return_value = mock_cluster_instance

        # Mock bucket and scope behavior
        mock_bucket = mock.Mock(spec=couchbase.bucket.Bucket)
        mock_cluster_instance.bucket.return_value = mock_bucket
        mock_scope = mock.Mock(spec=couchbase.scope.Scope)
        mock_bucket.scope.return_value = mock_scope
        mock_collection = mock.Mock(spec=couchbase.collection.Collection)
        mock_scope.collection.return_value = mock_collection
        
        hook = CouchbaseHook()
        collection = hook.get_collection(bucket="bucket", scope="scope", collection="collection")
        assert hook.cluster is not None
        mock_cluster.assert_called_once_with(
            "localhost",
            ClusterOptions(
                authenticator=couchbase.auth.PasswordAuthenticator(
                    username="username",
                    password="password",
                )
            ),
        )    
        mock_cluster_instance.bucket.assert_called_once_with("bucket")
        mock_bucket.scope.assert_called_once_with("scope")    
        mock_scope.collection.assert_called_once_with("collection")
        assert isinstance(collection, couchbase.collection.Collection), "collection is not of type Collection"