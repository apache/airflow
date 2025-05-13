from unittest.mock import Mock, patch

import pytest
from opendal import Operator

from airflow.providers.common.opendal.hooks.opendal import OpenDALHook


class TestOpenDALHook:

    def setup_method(self):
        """
        Setup method to initialize the OpenDALHook instance.
        """
        self.hook = OpenDALHook()

    def test_fetch_conn_with_opendal_input_config(self):
        """
        Test the fetch_conn method of OpenDALHook.
        """
        # Create a mock connection
        hook = OpenDALHook()
        hook.config = {
            "conn_id": "opendal_fs",
            "operator_args": {},
            "path": "/tmp/file/hello.txt"
        }
        conn = Mock()
        conn.extra_dejson = {
            "source_config": {
                "operator_args": {"key": "value"},
            },
            "destination_config": {
                "operator_args": {"key": "value"},
            },
        }

        hook.get_connection = Mock(return_value=conn)

        result = hook.fetch_conn()

        assert result == conn

    @pytest.mark.parametrize("config_type", ["source", "destination"])
    def test_fetch_conn_with_opendal_default_conn_id_with_source(self, config_type):
        """
        Test the fetch_conn method of OpenDALHook with default connection ID.
        """
        # Create a mock connection
        hook = OpenDALHook(config_type=config_type, opendal_conn_id="opendal_default")
        hook.config = {
            "operator_args": {},
            "path": "/tmp/file/hello.txt"
        }
        default_conn = Mock()
        default_conn.extra_dejson = {
            "source_config": {
                "conn_id": "aws_default",
                "operator_args": {"key": "value"},
            },
            "destination_config": {
                "conn_id": "aws_default",
                "operator_args": {"key": "value"},
            },
        }

        aws_default_conn = Mock()
        aws_default_conn.extra_dejson = {
                "aws_access_key_id": "test_aws_access_key_id",
                "aws_secret_access_key": "test_aws_secret_access_key",
                "region_name": "eu-central-1",
            }

        hook.get_connection = Mock(side_effect=[default_conn, aws_default_conn])
        result = hook.fetch_conn()

        assert result == aws_default_conn
        assert hook.get_connection.call_count == 2
        assert hook.get_connection.call_args_list[0][0][0] == "opendal_default"
        assert hook.get_connection.call_args_list[1][0][0] == "aws_default"

    @pytest.mark.parametrize("config_type", ["source", "destination"])
    @patch("airflow.providers.common.opendal.hooks.opendal.OpenDALHook.fetch_conn")
    def test_get_operator_use_operator_args_from_conn(self, mock_fetch_conn, config_type):
        """
        Test the get_operator method of OpenDALHook.
        """

        hook = OpenDALHook(config_type=config_type, opendal_conn_id="opendal_default")
        hook.config = {
            "operator_args": {},
            "path": "/tmp/file/hello.txt"
        }

        mock_fetch_conn.return_value = Mock(
            conn_type="opendal",
            extra_dejson={
                "source_config": {
                    "operator_args": {"scheme": "fs", "root": "/tmp/"},
                },
                "destination_config": {
                    "operator_args": {"scheme": "fs", "root": "/tmp/"},
                },
            }
        )
        operator = hook.get_operator
        assert isinstance(operator, Operator)
        assert repr(operator) == 'Operator("fs", root="/tmp")'

    @pytest.mark.parametrize("config_type", ["source", "destination"])
    @patch("airflow.providers.common.opendal.hooks.opendal.OpenDALHook.fetch_conn")
    def test_get_operator_use_operator_args_from_input_config(self, mock_fetch_conn, config_type):
        """
        Test the get_operator method of OpenDALHook.
        """

        hook = OpenDALHook(config_type=config_type, opendal_conn_id="opendal_default")
        hook.config = {
            "operator_args": {"scheme": "fs", "root": "/tmp/"},
            "path": "/tmp/file/hello.txt"
        }

        mock_fetch_conn.return_value = Mock(
            conn_type="opendal",
            extra_dejson={
                "source_config": {
                    "operator_args": {},
                },
                "destination_config": {
                    "operator_args": {},
                },
            }
        )
        operator = hook.get_operator
        assert isinstance(operator, Operator)
        assert repr(operator) == 'Operator("fs", root="/tmp")'









