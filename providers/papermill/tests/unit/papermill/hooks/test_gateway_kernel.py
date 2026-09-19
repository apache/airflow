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

import asyncio
import ssl
from unittest import mock

import pytest
from jupyter_server.gateway.gateway_client import GatewayClient
from papermill.clientwrap import PapermillNotebookClient

from airflow.providers.papermill.hooks.gateway_kernel import (
    GatewayKernelEngine,
    PapermillGatewayKernelClient,
    PapermillGatewayKernelManager,
    configure_gateway_client,
)


@pytest.fixture(autouse=True)
def _clear_gateway_client_singleton():
    GatewayClient.clear_instance()
    yield
    GatewayClient.clear_instance()


class TestConfigureGatewayClient:
    def test_sets_traits_on_singleton(self):
        gateway_client = configure_gateway_client(
            url="https://gateway.example.com:8888",
            token="s3cret",
            verify_ssl=False,
            ca_certs="/certs/ca.pem",
            request_timeout=120,
            connect_timeout=45,
            headers={"X-Custom": "1"},
        )

        assert gateway_client is GatewayClient.instance()
        assert gateway_client.url == "https://gateway.example.com:8888"
        assert gateway_client.ws_url == "wss://gateway.example.com:8888"
        assert gateway_client.auth_token == "s3cret"
        assert gateway_client.auth_scheme == "token"
        assert gateway_client.validate_cert is False
        assert gateway_client.ca_certs == "/certs/ca.pem"
        assert gateway_client.request_timeout == 120.0
        assert gateway_client.connect_timeout == 45.0
        assert gateway_client.headers == '{"X-Custom": "1"}'

    def test_derives_ws_url_from_http_url(self):
        gateway_client = configure_gateway_client(url="http://gateway.example.com:8888")

        assert gateway_client.ws_url == "ws://gateway.example.com:8888"
        assert gateway_client.validate_cert is True

    def test_rebuilds_singleton_so_prior_credentials_do_not_leak(self):
        configure_gateway_client(
            url="https://gateway-a.example.com",
            token="secret-a",
            ca_certs="/certs/a.pem",
            request_timeout=600,
        )
        # A second connection without a token/certs must not inherit the first one's.
        gateway_client = configure_gateway_client(url="https://gateway-b.example.com")

        assert gateway_client.auth_token == ""
        assert gateway_client.ca_certs is None
        assert gateway_client.request_timeout != 600
        # init_connection_args mutates this class attribute; it must start from the default too.
        assert GatewayClient.KERNEL_LAUNCH_TIMEOUT == 40


class TestGatewayKernelEngine:
    @pytest.fixture
    def engine_mocks(self):
        """Patch the manager and notebook client with spec'd mocks."""
        with (
            mock.patch(
                "airflow.providers.papermill.hooks.gateway_kernel.PapermillGatewayKernelManager"
            ) as km_cls,
            mock.patch(
                "airflow.providers.papermill.hooks.gateway_kernel.PapermillNotebookClient"
            ) as client_cls,
        ):
            km = mock.MagicMock(spec=PapermillGatewayKernelManager)
            km.start_kernel = mock.AsyncMock()
            km.shutdown_kernel = mock.AsyncMock()
            km.has_kernel = True
            km_cls.return_value = km

            client = mock.MagicMock(spec=PapermillNotebookClient)
            # kc is an instance attribute (not on the class spec), so set it explicitly.
            client.kc = mock.MagicMock()
            client_cls.return_value = client

            yield km_cls, km, client_cls, client

    def test_starts_kernel_and_cleans_up(self, engine_mocks):
        km_cls, km, client_cls, client = engine_mocks

        GatewayKernelEngine.execute_managed_notebook(
            mock.MagicMock(),
            "python3",
            gateway_url="http://gateway.example.com:8888",
            gateway_token="s3cret",
        )

        assert km.owns_kernel is True
        km.start_kernel.assert_awaited_once_with(kernel_id=None, kernel_name="python3")
        client_kwargs = client_cls.call_args.kwargs
        assert not any(key.startswith("gateway_") for key in client_kwargs)
        assert client_kwargs["km"] is km
        client.execute.assert_called_once_with(cleanup_kc=True)
        km.shutdown_kernel.assert_awaited_once_with(now=True)

    def test_attach_mode_keeps_kernel_alive(self, engine_mocks):
        km_cls, km, client_cls, client = engine_mocks

        GatewayKernelEngine.execute_managed_notebook(
            mock.MagicMock(),
            "python3",
            gateway_url="http://gateway.example.com:8888",
            gateway_kernel_id="abc",
        )

        assert km.owns_kernel is False
        km.start_kernel.assert_awaited_once_with(kernel_id="abc", kernel_name="python3")
        client.execute.assert_called_once_with(cleanup_kc=False)
        client.kc.stop_channels.assert_called_once()
        km.shutdown_kernel.assert_not_awaited()

    def test_attach_mode_missing_kernel_raises_without_starting_a_new_one(self, engine_mocks):
        km_cls, km, client_cls, client = engine_mocks
        km.has_kernel = False

        with pytest.raises(ValueError, match="was not found on the gateway"):
            GatewayKernelEngine.execute_managed_notebook(
                mock.MagicMock(),
                "python3",
                gateway_url="http://gateway.example.com:8888",
                gateway_kernel_id="abc",
            )

        client_cls.assert_not_called()
        km.shutdown_kernel.assert_not_awaited()

    def test_kernel_name_falls_back_to_notebook_metadata(self, engine_mocks):
        km_cls, km, client_cls, client = engine_mocks
        nb_man = mock.MagicMock()
        nb_man.nb.metadata = {"kernelspec": {"name": "spark"}}

        GatewayKernelEngine.execute_managed_notebook(
            nb_man, None, gateway_url="http://gateway.example.com:8888"
        )

        km.start_kernel.assert_awaited_once_with(kernel_id=None, kernel_name="spark")

    def test_shuts_down_kernel_when_execution_fails(self, engine_mocks):
        km_cls, km, client_cls, client = engine_mocks
        client.execute.side_effect = RuntimeError("cell failed")

        with pytest.raises(RuntimeError, match="cell failed"):
            GatewayKernelEngine.execute_managed_notebook(
                mock.MagicMock(), "python3", gateway_url="http://gateway.example.com:8888"
            )

        km.shutdown_kernel.assert_awaited_once_with(now=True)

    def test_shuts_down_kernel_when_client_construction_fails(self, engine_mocks):
        km_cls, km, client_cls, client = engine_mocks
        client_cls.side_effect = RuntimeError("bad kwargs")

        with pytest.raises(RuntimeError, match="bad kwargs"):
            GatewayKernelEngine.execute_managed_notebook(
                mock.MagicMock(), "python3", gateway_url="http://gateway.example.com:8888"
            )

        km.shutdown_kernel.assert_awaited_once_with(now=True)


class TestPapermillGatewayKernelManager:
    def test_shutdown_no_ops_for_attached_kernel(self):
        km = PapermillGatewayKernelManager()
        km.owns_kernel = False
        with mock.patch(
            "jupyter_server.gateway.managers.GatewayKernelManager.shutdown_kernel",
            new=mock.AsyncMock(),
        ) as base_shutdown:
            asyncio.run(km.shutdown_kernel(now=True))
        base_shutdown.assert_not_awaited()

    def test_shutdown_clears_kernel_model_for_owned_kernel(self):
        km = PapermillGatewayKernelManager()
        km.owns_kernel = True
        km.kernel = {"id": "abc"}
        with mock.patch(
            "jupyter_server.gateway.managers.GatewayKernelManager.shutdown_kernel",
            new=mock.AsyncMock(),
        ) as base_shutdown:
            asyncio.run(km.shutdown_kernel(now=True))
        base_shutdown.assert_awaited_once_with(now=True, restart=False)
        assert km.kernel is None


class TestPapermillGatewayKernelClient:
    @mock.patch("airflow.providers.papermill.hooks.gateway_kernel.Thread")
    @mock.patch("airflow.providers.papermill.hooks.gateway_kernel.websocket.create_connection")
    @mock.patch("jupyter_client.client.KernelClient.start_channels")
    def test_start_channels_sends_auth_header_and_ssl_options(
        self, mock_base_start_channels, mock_create_connection, mock_thread
    ):
        configure_gateway_client(url="https://gateway.example.com:8888", token="s3cret", verify_ssl=False)
        client = PapermillGatewayKernelClient(kernel_id="abc")

        asyncio.run(client.start_channels())

        ws_url = mock_create_connection.call_args.args[0]
        assert ws_url == "wss://gateway.example.com:8888/api/kernels/abc/channels"
        ws_kwargs = mock_create_connection.call_args.kwargs
        assert ws_kwargs["header"] == {"Authorization": "token s3cret"}
        assert ws_kwargs["sslopt"]["cert_reqs"] == ssl.CERT_NONE
        assert ws_kwargs["sslopt"]["check_hostname"] is False
        # The lifetime socket timeout is cleared so a quiet cell cannot time out the socket.
        mock_create_connection.return_value.settimeout.assert_called_once_with(None)
        mock_base_start_channels.assert_called_once()
        mock_thread.return_value.start.assert_called_once()

    @mock.patch("airflow.providers.papermill.hooks.gateway_kernel.Thread")
    @mock.patch("airflow.providers.papermill.hooks.gateway_kernel.websocket.create_connection")
    @mock.patch("jupyter_client.client.KernelClient.start_channels")
    def test_start_channels_validates_certs_by_default(
        self, mock_base_start_channels, mock_create_connection, mock_thread
    ):
        configure_gateway_client(url="https://gateway.example.com:8888", token="s3cret")
        client = PapermillGatewayKernelClient(kernel_id="abc")

        asyncio.run(client.start_channels())

        ws_kwargs = mock_create_connection.call_args.kwargs
        assert "cert_reqs" not in ws_kwargs["sslopt"]
        assert "check_hostname" not in ws_kwargs["sslopt"]

    @mock.patch("airflow.providers.papermill.hooks.gateway_kernel.websocket.create_connection")
    @mock.patch("jupyter_client.client.KernelClient.start_channels")
    def test_start_channels_closes_socket_when_setup_fails(
        self, mock_base_start_channels, mock_create_connection
    ):
        mock_base_start_channels.side_effect = RuntimeError("handshake dropped")
        configure_gateway_client(url="https://gateway.example.com:8888", token="s3cret")
        client = PapermillGatewayKernelClient(kernel_id="abc")

        with pytest.raises(RuntimeError, match="handshake dropped"):
            asyncio.run(client.start_channels())

        mock_create_connection.return_value.close.assert_called_once()

    @mock.patch("jupyter_client.client.KernelClient.stop_channels")
    def test_stop_channels_tolerates_partial_start(self, mock_base_stop_channels):
        client = PapermillGatewayKernelClient(kernel_id="abc")
        # start_channels never ran, so channel_socket and response_router are still None.
        client.stop_channels()
        mock_base_stop_channels.assert_called_once()
