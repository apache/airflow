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

import os
from unittest import mock
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest
from requests import Session as request_session
from simple_salesforce import Salesforce, api

from airflow.models.connection import Connection
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook
from airflow.utils.session import create_session


class TestSalesforceHook:
    def setup_method(self):
        self.salesforce_hook = SalesforceHook(salesforce_conn_id="conn_id")

    @staticmethod
    def _insert_conn_db_entry(conn_id, conn_object):
        with create_session() as session:
            session.query(Connection).filter(Connection.conn_id == conn_id).delete()
            session.add(conn_object)
            session.commit()

    def test_get_conn_exists(self):
        self.salesforce_hook.conn = Mock(spec=Salesforce)

        self.salesforce_hook.get_conn()

        assert self.salesforce_hook.conn.return_value is not None

    @pytest.mark.db_test
    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_get_conn_password_auth(self, mock_salesforce):
        """
        Testing mock password authentication to Salesforce. Users should provide a username, password, and
        security token in the Connection. Providing a client ID, Salesforce API version, proxy mapping, and
        domain are optional. Connection params not provided or set as empty strings should be converted to
        `None`.
        """

        password_auth_conn = Connection(
            conn_id="password_auth_conn",
            conn_type="salesforce",
            login=None,
            password=None,
            extra="""
            {
                "client_id": "my_client",
                "domain": "test",
                "security_token": "token",
                "version": "42.0"
            }
            """,
        )
        TestSalesforceHook._insert_conn_db_entry(
            password_auth_conn.conn_id, password_auth_conn
        )

        self.salesforce_hook = SalesforceHook(salesforce_conn_id="password_auth_conn")
        self.salesforce_hook.get_conn()

        extras = password_auth_conn.extra_dejson
        mock_salesforce.assert_called_once_with(
            username=password_auth_conn.login,
            password=password_auth_conn.password,
            security_token=extras["security_token"],
            domain=extras["domain"],
            session_id=None,
            instance=None,
            instance_url=None,
            organizationId=None,
            version=extras["version"],
            proxies=None,
            session=None,
            client_id=extras["client_id"],
            consumer_key=None,
            privatekey_file=None,
            privatekey=None,
        )

    @pytest.mark.db_test
    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_get_conn_direct_session_access(self, mock_salesforce):
        """
        Testing mock direct session access to Salesforce. Users should provide an instance
        (or instance URL) in the Connection and set a `session_id` value when calling `SalesforceHook`.
        Providing a client ID, Salesforce API version, proxy mapping, and domain are optional. Connection
        params not provided or set as empty strings should be converted to `None`.
        """

        direct_access_conn = Connection(
            conn_id="direct_access_conn",
            conn_type="salesforce",
            login=None,
            password=None,
            extra="""
            {
                "client_id": "my_client2",
                "domain": "test",
                "instance_url": "https://my.salesforce.com",
                "version": "29.0"
            }
            """,
        )
        TestSalesforceHook._insert_conn_db_entry(
            direct_access_conn.conn_id, direct_access_conn
        )

        with request_session() as session:
            self.salesforce_hook = SalesforceHook(
                salesforce_conn_id="direct_access_conn",
                session_id="session_id",
                session=session,
            )

        self.salesforce_hook.get_conn()

        extras = direct_access_conn.extra_dejson
        mock_salesforce.assert_called_once_with(
            username=direct_access_conn.login,
            password=direct_access_conn.password,
            security_token=None,
            domain=extras["domain"],
            session_id=self.salesforce_hook.session_id,
            instance=None,
            instance_url=extras["instance_url"],
            organizationId=None,
            version=extras["version"],
            proxies=None,
            session=self.salesforce_hook.session,
            client_id=extras["client_id"],
            consumer_key=None,
            privatekey_file=None,
            privatekey=None,
        )

    @pytest.mark.db_test
    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_get_conn_jwt_auth(self, mock_salesforce):
        """
        Testing mock JWT bearer authentication to Salesforce. Users should provide consumer key and private
        key (or path to a private key) in the Connection. Providing a client ID, Salesforce API version, proxy
        mapping, and domain are optional. Connection params not provided or set as empty strings should be
        converted to `None`.
        """

        jwt_auth_conn = Connection(
            conn_id="jwt_auth_conn",
            conn_type="salesforce",
            login=None,
            password=None,
            extra="""
            {
                "client_id": "my_client3",
                "consumer_key": "consumer_key",
                "domain": "login",
                "private_key": "private_key",
                "version": "34.0"
            }
            """,
        )
        TestSalesforceHook._insert_conn_db_entry(jwt_auth_conn.conn_id, jwt_auth_conn)

        self.salesforce_hook = SalesforceHook(salesforce_conn_id="jwt_auth_conn")
        self.salesforce_hook.get_conn()

        extras = jwt_auth_conn.extra_dejson
        mock_salesforce.assert_called_once_with(
            username=jwt_auth_conn.login,
            password=jwt_auth_conn.password,
            security_token=None,
            domain=extras["domain"],
            session_id=None,
            instance=None,
            instance_url=None,
            organizationId=None,
            version=extras["version"],
            proxies=None,
            session=None,
            client_id=extras["client_id"],
            consumer_key=extras["consumer_key"],
            privatekey_file=None,
            privatekey=extras["private_key"],
        )

    @pytest.mark.db_test
    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_get_conn_ip_filtering_auth(self, mock_salesforce):
        """
        Testing mock IP filtering (aka allow-listing) authentication to Salesforce. Users should provide
        username, password, and organization ID in the Connection. Providing a client ID, Salesforce API
        version, proxy mapping, and domain are optional. Connection params not provided or set as empty
        strings should be converted to `None`.
        """

        ip_filtering_auth_conn = Connection(
            conn_id="ip_filtering_auth_conn",
            conn_type="salesforce",
            login="username",
            password="password",
            extra="""
            {
                "organization_id": "my_organization"
            }
            """,
        )
        TestSalesforceHook._insert_conn_db_entry(
            ip_filtering_auth_conn.conn_id, ip_filtering_auth_conn
        )

        self.salesforce_hook = SalesforceHook(salesforce_conn_id="ip_filtering_auth_conn")
        self.salesforce_hook.get_conn()

        extras = ip_filtering_auth_conn.extra_dejson
        mock_salesforce.assert_called_once_with(
            username=ip_filtering_auth_conn.login,
            password=ip_filtering_auth_conn.password,
            security_token=None,
            domain=None,
            session_id=None,
            instance=None,
            instance_url=None,
            organizationId=extras["organization_id"],
            version=api.DEFAULT_API_VERSION,
            proxies=None,
            session=None,
            client_id=None,
            consumer_key=None,
            privatekey_file=None,
            privatekey=None,
        )

    @pytest.mark.db_test
    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_get_conn_default_to_none(self, mock_salesforce):
        """
        Testing mock authentication to Salesforce so that every extra connection param set as an empty
        string will be converted to `None`.
        """

        default_to_none_conn = Connection(
            conn_id="default_to_none_conn",
            conn_type="salesforce",
            login=None,
            password=None,
            extra="""
            {
                "client_id": "",
                "consumer_key": "",
                "domain": "",
                "instance": "",
                "instance_url": "",
                "organization_id": "",
                "private_key": "",
                "private_key_file_path": "",
                "proxies": "",
                "security_token": ""
            }
            """,
        )
        TestSalesforceHook._insert_conn_db_entry(
            default_to_none_conn.conn_id, default_to_none_conn
        )

        self.salesforce_hook = SalesforceHook(salesforce_conn_id="default_to_none_conn")
        self.salesforce_hook.get_conn()

        mock_salesforce.assert_called_once_with(
            username=default_to_none_conn.login,
            password=default_to_none_conn.password,
            security_token=None,
            domain=None,
            session_id=None,
            instance=None,
            instance_url=None,
            organizationId=None,
            version=api.DEFAULT_API_VERSION,
            proxies=None,
            session=None,
            client_id=None,
            consumer_key=None,
            privatekey_file=None,
            privatekey=None,
        )

    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_make_query(self, mock_salesforce):
        mock_salesforce.return_value.query_all.return_value = dict(
            totalSize=123, done=True
        )
        self.salesforce_hook.conn = mock_salesforce.return_value
        query = "SELECT * FROM table"

        query_results = self.salesforce_hook.make_query(query, include_deleted=True)

        mock_salesforce.return_value.query_all.assert_called_once_with(
            query, include_deleted=True
        )
        assert query_results == mock_salesforce.return_value.query_all.return_value

    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_describe_object(self, mock_salesforce):
        obj = "obj_name"
        mock_salesforce.return_value.__setattr__(obj, Mock(spec=Salesforce))
        self.salesforce_hook.conn = mock_salesforce.return_value

        obj_description = self.salesforce_hook.describe_object(obj)

        mock_salesforce.return_value.__getattr__(obj).describe.assert_called_once_with()
        assert (
            obj_description
            == mock_salesforce.return_value.__getattr__(obj).describe.return_value
        )

    @patch(
        "airflow.providers.salesforce.hooks.salesforce.SalesforceHook.describe_object",
        return_value={"fields": [{"name": "field_1"}, {"name": "field_2"}]},
    )
    def test_get_available_fields(self, mock_describe_object):
        obj = "obj_name"

        available_fields = self.salesforce_hook.get_available_fields(obj)

        mock_describe_object.assert_called_once_with(obj)
        assert available_fields == ["field_1", "field_2"]

    @patch("airflow.providers.salesforce.hooks.salesforce.SalesforceHook.make_query")
    def test_get_object_from_salesforce(self, mock_make_query):
        salesforce_objects = self.salesforce_hook.get_object_from_salesforce(
            obj="obj_name", fields=["field_1", "field_2"]
        )

        mock_make_query.assert_called_once_with("SELECT field_1,field_2 FROM obj_name")
        assert salesforce_objects == mock_make_query.return_value

    def test_write_object_to_file_invalid_format(self):
        with pytest.raises(ValueError):
            self.salesforce_hook.write_object_to_file(
                query_results=[], filename="test", fmt="test"
            )

    @patch(
        "pandas.DataFrame.from_records",
        return_value=pd.DataFrame(
            {"test": [1, 2, 3], "dict": [np.nan, np.nan, {"foo": "bar"}]}
        ),
    )
    def test_write_object_to_file_csv(self, mock_data_frame):
        mock_data_frame.return_value.to_csv = Mock()
        filename = "test"

        data_frame = self.salesforce_hook.write_object_to_file(
            query_results=[], filename=filename, fmt="csv"
        )

        mock_data_frame.return_value.to_csv.assert_called_once_with(filename, index=False)
        # Note that the latest version of pandas dataframes (1.1.2) returns "nan" rather than "None" here
        pd.testing.assert_frame_equal(
            data_frame,
            pd.DataFrame(
                {"test": [1, 2, 3], "dict": ["nan", "nan", str({"foo": "bar"})]}
            ),
            check_index_type=False,
        )

    @patch(
        "airflow.providers.salesforce.hooks.salesforce.SalesforceHook.describe_object",
        return_value={"fields": [{"name": "field_1", "type": "date"}]},
    )
    @patch(
        "pandas.DataFrame.from_records",
        return_value=pd.DataFrame(
            {"test": [1, 2, 3], "field_1": ["2019-01-01", "2019-01-02", "2019-01-03"]}
        ),
    )
    def test_write_object_to_file_json_with_timestamp_conversion(
        self, mock_data_frame, mock_describe_object
    ):
        mock_data_frame.return_value.to_json = Mock()
        filename = "test"
        obj_name = "obj_name"

        data_frame = self.salesforce_hook.write_object_to_file(
            query_results=[{"attributes": {"type": obj_name}}],
            filename=filename,
            fmt="json",
            coerce_to_timestamp=True,
        )

        mock_describe_object.assert_called_once_with(obj_name)
        mock_data_frame.return_value.to_json.assert_called_once_with(
            filename, "records", date_unit="s"
        )
        pd.testing.assert_frame_equal(
            data_frame,
            pd.DataFrame(
                {"test": [1, 2, 3], "field_1": [1.546301e09, 1.546387e09, 1.546474e09]}
            ),
        )

    @patch("airflow.providers.salesforce.hooks.salesforce.time.time", return_value=1.23)
    @patch(
        "pandas.DataFrame.from_records",
        return_value=pd.DataFrame({"test": [1, 2, 3]}),
    )
    def test_write_object_to_file_ndjson_with_record_time(
        self, mock_data_frame, mock_time
    ):
        mock_data_frame.return_value.to_json = Mock()
        filename = "test"

        data_frame = self.salesforce_hook.write_object_to_file(
            query_results=[], filename=filename, fmt="ndjson", record_time_added=True
        )

        mock_data_frame.return_value.to_json.assert_called_once_with(
            filename, "records", lines=True, date_unit="s"
        )
        pd.testing.assert_frame_equal(
            data_frame,
            pd.DataFrame(
                {
                    "test": [1, 2, 3],
                    "time_fetched_from_salesforce": [
                        mock_time.return_value,
                        mock_time.return_value,
                        mock_time.return_value,
                    ],
                }
            ),
        )

    @patch(
        "airflow.providers.salesforce.hooks.salesforce.SalesforceHook.describe_object",
        return_value={"fields": [{"name": "field_1", "type": "date"}]},
    )
    @patch(
        "pandas.DataFrame.from_records",
        return_value=pd.DataFrame(
            {
                "test": [1, 2, 3, 4],
                "field_1": ["2019-01-01", "2019-01-02", "2019-01-03", "NaT"],
            }
        ),
    )
    def test_object_to_df_with_timestamp_conversion(
        self, mock_data_frame, mock_describe_object
    ):
        obj_name = "obj_name"

        data_frame = self.salesforce_hook.object_to_df(
            query_results=[{"attributes": {"type": obj_name}}],
            coerce_to_timestamp=True,
        )

        mock_describe_object.assert_called_once_with(obj_name)
        pd.testing.assert_frame_equal(
            data_frame,
            pd.DataFrame(
                {
                    "test": [1, 2, 3, 4],
                    "field_1": [1.546301e09, 1.546387e09, 1.546474e09, np.nan],
                }
            ),
        )

    @patch("airflow.providers.salesforce.hooks.salesforce.time.time", return_value=1.23)
    @patch(
        "pandas.DataFrame.from_records",
        return_value=pd.DataFrame({"test": [1, 2, 3]}),
    )
    def test_object_to_df_with_record_time(self, mock_data_frame, mock_time):
        data_frame = self.salesforce_hook.object_to_df(
            query_results=[], record_time_added=True
        )

        pd.testing.assert_frame_equal(
            data_frame,
            pd.DataFrame(
                {
                    "test": [1, 2, 3],
                    "time_fetched_from_salesforce": [
                        mock_time.return_value,
                        mock_time.return_value,
                        mock_time.return_value,
                    ],
                }
            ),
        )

    @pytest.mark.parametrize(
        "uri",
        [
            pytest.param(
                "a://?extra__salesforce__security_token=token&extra__salesforce__domain=domain",
                id="prefix",
            ),
            pytest.param("a://?security_token=token&domain=domain", id="no-prefix"),
        ],
    )
    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_backcompat_prefix_works(self, mock_client, uri):
        with patch.dict(os.environ, {"AIRFLOW_CONN_MY_CONN": uri}):
            hook = SalesforceHook("my_conn")
            hook.get_conn()
            mock_client.assert_called_with(
                client_id=None,
                consumer_key=None,
                domain="domain",
                instance=None,
                instance_url=None,
                organizationId=None,
                password=None,
                privatekey=None,
                privatekey_file=None,
                proxies=None,
                security_token="token",
                session=None,
                session_id=None,
                username=None,
                version=mock.ANY,
            )

    @patch("airflow.providers.salesforce.hooks.salesforce.Salesforce")
    def test_backcompat_prefix_both_prefers_short(self, mock_client):
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_CONN_MY_CONN": "a://?security_token=non-prefixed"
                "&extra__salesforce__security_token=prefixed"
            },
        ):
            hook = SalesforceHook("my_conn")
            hook.get_conn()
            mock_client.assert_called_with(
                client_id=None,
                consumer_key=None,
                domain=None,
                instance=None,
                instance_url=None,
                organizationId=None,
                password=None,
                privatekey=None,
                privatekey_file=None,
                proxies=None,
                security_token="non-prefixed",
                session=None,
                session_id=None,
                username=None,
                version=mock.ANY,
            )

    @patch(
        "airflow.providers.salesforce.hooks.salesforce.SalesforceHook.describe_object",
        return_value={"fields": [{"name": "field_1"}, {"name": "field_2"}]},
    )
    def test_connection_success(self, mock_describe_object):
        hook = SalesforceHook("my_conn")
        status, msg = hook.test_connection()
        assert status is True
        assert msg == "Connection successfully tested"

    @patch(
        "airflow.providers.salesforce.hooks.salesforce.SalesforceHook.describe_object",
        side_effect=Exception("Test"),
    )
    def test_connection_failure(self, mock_describe_object):
        hook = SalesforceHook("my_conn")
        status, msg = hook.test_connection()
        assert status is False
        assert msg == "Test"
