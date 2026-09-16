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

import json
import ssl
import typing
from threading import Thread

import websocket
from jupyter_core.utils import ensure_async, run_sync
from jupyter_server.gateway.gateway_client import GatewayClient
from jupyter_server.gateway.managers import GatewayKernelClient, GatewayKernelManager
from jupyter_server.utils import url_path_join
from papermill.clientwrap import PapermillNotebookClient
from papermill.engines import NBClientEngine
from papermill.utils import merge_kwargs, remove_args
from tornado.escape import url_escape
from traitlets import Bool, DottedObjectName, Type

# init_connection_args mutates this class attribute from request_timeout; keep the pristine
# default so one run's timeout cannot leak into the next run in the same process.
_DEFAULT_KERNEL_LAUNCH_TIMEOUT = GatewayClient.KERNEL_LAUNCH_TIMEOUT


class PapermillGatewayKernelClient(GatewayKernelClient):
    """
    Kernel client that authenticates the WebSocket connection to the gateway.

    ``jupyter_server``'s ``GatewayKernelClient.start_channels`` opens the kernel channels
    WebSocket without the authorization/cookie headers that ``load_connection_args`` applies
    to REST calls (so token-secured servers reply with 403) and without honoring
    ``validate_cert=False``. This subclass re-implements ``start_channels`` with both fixed.
    """

    async def start_channels(self, shell=True, iopub=True, stdin=True, hb=True, control=True):
        """Start the channels for this kernel over an authenticated WebSocket connection."""
        gateway_client = GatewayClient.instance()
        ws_url = url_path_join(
            gateway_client.ws_url or "",
            gateway_client.kernels_endpoint,
            url_escape(typing.cast("str", self.kernel_id)),
            "channels",
        )
        ssl_options: dict[str, typing.Any] = {
            "ca_certs": gateway_client.ca_certs,
            "certfile": gateway_client.client_cert,
            "keyfile": gateway_client.client_key,
        }
        if not gateway_client.validate_cert:
            ssl_options.update(cert_reqs=ssl.CERT_NONE, check_hostname=False)
        # load_connection_args runs the configured token renewer and attaches session cookies,
        # exactly as the REST calls do; building the header by hand would skip both.
        headers = gateway_client.load_connection_args().get("headers") or {}

        self.channel_socket = websocket.create_connection(
            ws_url,
            timeout=gateway_client.KERNEL_LAUNCH_TIMEOUT,
            enable_multithread=True,
            sslopt=ssl_options,
            header=headers,
        )
        try:
            # The timeout above bounds the handshake, but create_connection installs it as the
            # socket timeout for the connection's lifetime — a cell quiet for longer than it
            # would kill the response-router thread mid-run. Message-level timeouts are
            # enforced by nbclient, so the socket itself can block indefinitely.
            self.channel_socket.settimeout(None)

            # Skip GatewayKernelClient.start_channels (re-implemented above) and resume the MRO
            # at AsyncKernelClient to set up the channel-based queues.
            await ensure_async(
                super(GatewayKernelClient, self).start_channels(
                    shell=shell, iopub=iopub, stdin=stdin, hb=hb, control=control
                )
            )

            self.response_router = Thread(target=self._route_responses)
            self.response_router.start()
        except BaseException:
            self.channel_socket.close()
            raise

    def stop_channels(self):
        """
        Stop the channels, tolerating a partially-started state.

        Upstream asserts ``channel_socket``/``response_router`` are set, so a failure during
        ``start_channels`` would surface as ``AssertionError`` from cleanup instead of the
        real error.
        """
        super(GatewayKernelClient, self).stop_channels()
        if not self._channels_stopped:
            self._channels_stopped = True
            if self.channel_socket is not None:
                self.channel_socket.close()
            if self.response_router is not None:
                self.response_router.join()


class PapermillGatewayKernelManager(GatewayKernelManager):
    """Gateway kernel manager whose clients authenticate the WebSocket connection."""

    # GatewayKernelManager pins client_factory explicitly, so both traits must be overridden.
    client_class = DottedObjectName(
        "airflow.providers.papermill.hooks.gateway_kernel.PapermillGatewayKernelClient"
    )
    client_factory = Type(klass=PapermillGatewayKernelClient)

    owns_kernel = Bool(
        True,
        help="Whether this manager started the kernel. When attached to a pre-existing kernel "
        "(kernel_id connection extra), shutdown becomes a no-op so no cleanup path — including "
        "nbclient's unconditional cleanup on client-startup failure — can delete the user's kernel.",
    )

    async def shutdown_kernel(self, now=False, restart=False):
        """Shut down the kernel, unless it is a pre-existing kernel this manager attached to."""
        if not self.owns_kernel:
            return
        await super().shutdown_kernel(now=now, restart=restart)
        # Upstream never clears the model, so has_kernel would stay True after shutdown and a
        # second DELETE would be sent by any later cleanup path.
        self.kernel = None


def configure_gateway_client(
    *,
    url: str,
    token: str | None = None,
    auth_scheme: str = "token",
    auth_header_key: str = "Authorization",
    verify_ssl: bool = True,
    ca_certs: str | None = None,
    client_cert: str | None = None,
    client_key: str | None = None,
    request_timeout: float | None = None,
    connect_timeout: float | None = None,
    headers: dict | None = None,
) -> GatewayClient:
    """Configure the process-wide ``GatewayClient`` singleton from connection details."""
    # Rebuild the singleton from scratch: partially overwriting a previously configured
    # instance would leak the earlier connection's token, certificates, headers, and timeouts
    # into this run (e.g. consecutive tasks under ``dag.test()``).
    GatewayClient.clear_instance()
    GatewayClient.KERNEL_LAUNCH_TIMEOUT = _DEFAULT_KERNEL_LAUNCH_TIMEOUT
    gateway_client = GatewayClient.instance()
    gateway_client.url = url
    # The trait default derives ws_url with str.replace("http", "ws"), which also corrupts
    # any "http" occurring later in the URL path.
    if url.startswith("https://"):
        gateway_client.ws_url = f"wss://{url[len('https://') :]}"
    elif url.startswith("http://"):
        gateway_client.ws_url = f"ws://{url[len('http://') :]}"
    if token:
        gateway_client.auth_token = token
    gateway_client.auth_scheme = auth_scheme
    gateway_client.auth_header_key = auth_header_key
    gateway_client.validate_cert = verify_ssl
    if ca_certs:
        gateway_client.ca_certs = ca_certs
    if client_cert:
        gateway_client.client_cert = client_cert
    if client_key:
        gateway_client.client_key = client_key
    if request_timeout is not None:
        gateway_client.request_timeout = float(request_timeout)
    if connect_timeout is not None:
        gateway_client.connect_timeout = float(connect_timeout)
    if headers:
        gateway_client.headers = json.dumps(headers)
    return gateway_client


class GatewayKernelEngine(NBClientEngine):
    """Papermill engine that executes a notebook on a kernel behind a Jupyter server/gateway over HTTP(S)."""

    @classmethod
    def execute_managed_notebook(
        cls,
        nb_man,
        kernel_name,
        log_output=False,
        stdout_file=None,
        stderr_file=None,
        start_timeout=60,
        execution_timeout=None,
        **kwargs,
    ):
        """Perform the actual execution of the parameterized notebook on a gateway-managed kernel."""
        gateway_kwargs = {
            key[len("gateway_") :]: kwargs.pop(key) for key in list(kwargs) if key.startswith("gateway_")
        }
        kernel_id = gateway_kwargs.pop("kernel_id", None)
        configure_gateway_client(**gateway_kwargs)

        owns_kernel = kernel_id is None
        km = PapermillGatewayKernelManager()
        # The manager's __init__ chain drops constructor kwargs, so set the trait directly.
        km.owns_kernel = owns_kernel
        resolved_kernel_name = (
            kernel_name or nb_man.nb.metadata.get("kernelspec", {}).get("name") or "python3"
        )

        # Exclude parameters that named differently downstream
        safe_kwargs = remove_args(["timeout", "startup_timeout"], **kwargs)

        final_kwargs = merge_kwargs(
            safe_kwargs,
            timeout=execution_timeout if execution_timeout else kwargs.get("timeout"),
            startup_timeout=start_timeout,
            log_output=log_output,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
        )

        client = None
        try:
            run_sync(km.start_kernel)(kernel_id=kernel_id, kernel_name=resolved_kernel_name)
            if not km.has_kernel:
                # Attaching to a culled/unknown kernel_id: the gateway 404 is swallowed
                # upstream, and nbclient would silently start (and leak) a new kernel with
                # the default kernel name instead.
                raise ValueError(
                    f"Kernel {kernel_id!r} was not found on the gateway at "
                    f"{GatewayClient.instance().url}; remove the 'kernel_id' connection extra "
                    "to start a new kernel instead."
                )
            client = PapermillNotebookClient(nb_man, km=km, **final_kwargs)
            # nbclient only cleans up kernels it started itself (owns_km); cleanup_kc=True makes
            # it shut down the kernel we started here even when cells fail, so no kernels leak
            # on the gateway. The attached-kernel case is protected by owns_kernel above.
            return client.execute(cleanup_kc=owns_kernel)
        finally:
            if owns_kernel:
                # Safety net for failures before/outside nbclient's own cleanup (e.g. the
                # kernel started but client construction raised). shutdown_kernel clears the
                # kernel model, so this is a no-op when nbclient already shut it down.
                if km.has_kernel:
                    run_sync(km.shutdown_kernel)(now=True)
            elif client is not None and client.kc is not None:
                # Stop the WebSocket response-router thread (non-daemon) without touching the kernel.
                client.kc.stop_channels()
