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
import resource
import socket
import threading

import paramiko
import pytest


class _InProcessServer(paramiko.ServerInterface):
    """
    Answer exec requests and echo forwarded (``direct-tcpip``) connections.

    Every exec request gets stdout, stderr and exit status 3. A forwarded channel echoes each chunk
    it receives, and the server closes it after the first one when ``close_forwarded_after_echo``
    is set.
    """

    def __init__(self) -> None:
        self.forwarded_chanids: set[int] = set()
        self.close_forwarded_after_echo = False

    def get_allowed_auths(self, username):
        return "password"

    def check_auth_password(self, username, password):
        return paramiko.AUTH_SUCCESSFUL

    def check_channel_request(self, kind, chanid):
        return paramiko.OPEN_SUCCEEDED

    def check_channel_exec_request(self, channel, command):
        def respond():
            # Give the client time to see the exec request succeed before the channel closes.
            threading.Event().wait(0.2)
            channel.sendall(b"out-1\n")
            channel.sendall_stderr(b"err-1\n")
            channel.sendall(b"out-2\n")
            channel.send_exit_status(3)
            channel.close()

        threading.Thread(target=respond, daemon=True).start()
        return True

    def check_channel_direct_tcpip_request(self, chanid, origin, destination):
        self.forwarded_chanids.add(chanid)
        return paramiko.OPEN_SUCCEEDED

    def echo(self, channel: paramiko.Channel) -> None:
        while data := channel.recv(16384):
            channel.sendall(data)
            if self.close_forwarded_after_echo:
                break
        channel.close()


@pytest.fixture
def in_process_ssh_server():
    """Yield the in-process paramiko server behind ``in_process_ssh_client``."""
    return _InProcessServer()


@pytest.fixture
def in_process_ssh_client(in_process_ssh_server):
    """Yield an SSH client connected to an in-process paramiko server over a loopback socket."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    host_key = paramiko.ECDSAKey.generate()
    transports = []

    def serve():
        sock, _ = listener.accept()
        transport = paramiko.Transport(sock)
        transport.add_server_key(host_key)
        transport.start_server(server=in_process_ssh_server)
        transports.append(transport)
        while transport.is_active():
            channel = transport.accept(timeout=0.5)
            if channel is not None and channel.chanid in in_process_ssh_server.forwarded_chanids:
                threading.Thread(target=in_process_ssh_server.echo, args=(channel,), daemon=True).start()

    server_thread = threading.Thread(target=serve, daemon=True)
    server_thread.start()
    client = paramiko.SSHClient()
    # Trust exactly the server's key; any other key is rejected by the default policy.
    client.get_host_keys().add(f"[127.0.0.1]:{port}", host_key.get_name(), host_key)
    client.connect(
        "127.0.0.1",
        port=port,
        username="user",
        password="password",
        look_for_keys=False,
        allow_agent=False,
    )
    yield client
    client.close()
    server_thread.join(timeout=10)
    for transport in transports:
        transport.close()
    listener.close()


@pytest.fixture
def over_1024_open_fds():
    """Hold enough descriptors that new ones are numbered above select()'s FD_SETSIZE of 1024."""
    count = 1100
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < count + 256:
        if hard != resource.RLIM_INFINITY and hard < count + 256:
            pytest.skip(f"RLIMIT_NOFILE hard limit {hard} is too low to open {count} descriptors")
        resource.setrlimit(resource.RLIMIT_NOFILE, (count + 256, hard))
    fds = [os.open(os.devnull, os.O_RDONLY) for _ in range(count)]
    assert max(fds) > 1024
    yield
    for fd in fds:
        os.close(fd)
    resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
