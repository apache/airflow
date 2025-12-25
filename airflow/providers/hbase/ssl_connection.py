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
"""Custom HappyBase Connection with SSL support."""

import ssl
import tempfile

import happybase
from thriftpy2.transport import TSSLSocket, TFramedTransport
from thriftpy2.protocol import TBinaryProtocol
from thriftpy2.thrift import TClient

from airflow.models import Variable


class SSLHappyBaseConnection(happybase.Connection):
    """HappyBase Connection with SSL support.

    This class extends the standard happybase.Connection to support SSL/TLS connections.
    HappyBase doesn't support SSL by default, so we override the Thrift client creation.

    Key features:
    1. Creates SSL context for secure connections
    2. Uses TSSLSocket instead of regular socket
    3. Configures Thrift transport with SSL support
    4. Manages temporary certificate files
    5. Ensures compatibility with HBase Thrift API
    """

    def __init__(self, ssl_context=None, **kwargs):
        """Initialize SSL connection.

        Args:
            ssl_context: SSL context for connection encryption
            **kwargs: Other parameters for happybase.Connection
        """
        self.ssl_context = ssl_context
        self._temp_cert_files = []  # List of temporary certificate files for cleanup
        super().__init__(**kwargs)

    def _refresh_thrift_client(self):
        """Override Thrift client creation to use SSL.

        This is the key method that replaces standard TCP connection with SSL.
        HappyBase uses Thrift to communicate with HBase, and we intercept this process.

        Process:
        1. Create TSSLSocket with SSL context instead of regular socket
        2. Wrap in TFramedTransport (required by HBase)
        3. Create TBinaryProtocol for data serialization
        4. Create TClient with proper HBase Thrift interface
        """
        if self.ssl_context:
            # Create SSL socket with encryption
            socket = TSSLSocket(
                host=self.host,
                port=self.port,
                ssl_context=self.ssl_context
            )

            # Create framed transport (mandatory for HBase)
            # HBase requires framed protocol for correct operation
            self.transport = TFramedTransport(socket)

            # Create binary protocol for Thrift message serialization
            protocol = TBinaryProtocol(self.transport, decode_response=False)

            # Create Thrift client with proper HBase interface
            from happybase.connection import Hbase
            self.client = TClient(Hbase, protocol)
        else:
            # Use standard implementation without SSL
            super()._refresh_thrift_client()

    def open(self):
        """Open SSL connection.

        Check if transport is not open and open it.
        SSL handshake happens automatically when opening TSSLSocket.
        """
        if not self.transport.is_open():
            self.transport.open()

    def close(self):
        """Close connection and cleanup temporary files.

        Important to clean up temporary certificate files for security.
        """
        super().close()
        self._cleanup_temp_files()

    def _cleanup_temp_files(self):
        """Clean up temporary certificate files.

        Remove all temporary files created for storing certificates.
        This is important for security - certificates should not remain on disk.
        """
        import os
        for temp_file in self._temp_cert_files:
            try:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
            except Exception:
                pass  # Ignore errors during deletion
        self._temp_cert_files.clear()


def create_ssl_connection(host, port, ssl_config, **kwargs):
    """Create SSL-enabled HappyBase connection."""
    if not ssl_config.get("use_ssl", False):
        return happybase.Connection(host=host, port=port, **kwargs)

    # Create SSL context
    ssl_context = ssl.create_default_context()

    # Configure SSL verification
    verify_mode = ssl_config.get("ssl_verify_mode", "CERT_REQUIRED")
    if verify_mode == "CERT_NONE":
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
    elif verify_mode == "CERT_OPTIONAL":
        ssl_context.verify_mode = ssl.CERT_OPTIONAL
    else:
        ssl_context.verify_mode = ssl.CERT_REQUIRED

    # Load certificates from Variables
    temp_files = []

    if ssl_config.get("ssl_ca_secret"):
        ca_cert_content = Variable.get(ssl_config["ssl_ca_secret"], None)
        if ca_cert_content:
            ca_cert_file = tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False)
            ca_cert_file.write(ca_cert_content)
            ca_cert_file.close()
            ssl_context.load_verify_locations(cafile=ca_cert_file.name)
            temp_files.append(ca_cert_file.name)

    if ssl_config.get("ssl_cert_secret") and ssl_config.get("ssl_key_secret"):
        cert_content = Variable.get(ssl_config["ssl_cert_secret"], None)
        key_content = Variable.get(ssl_config["ssl_key_secret"], None)

        if cert_content and key_content:
            cert_file = tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False)
            cert_file.write(cert_content)
            cert_file.close()

            key_file = tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False)
            key_file.write(key_content)
            key_file.close()

            ssl_context.load_cert_chain(certfile=cert_file.name, keyfile=key_file.name)
            temp_files.extend([cert_file.name, key_file.name])

    # Create SSL connection
    connection = SSLHappyBaseConnection(
        host=host,
        port=port,
        ssl_context=ssl_context,
        **kwargs
    )
    connection._temp_cert_files = temp_files

    return connection
