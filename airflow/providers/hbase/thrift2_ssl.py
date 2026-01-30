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
"""SSL utilities for Thrift2 connections."""

from __future__ import annotations

import ssl
import tempfile
from typing import Any

from airflow.models import Variable


def create_ssl_context(ssl_config: dict[str, Any]) -> tuple[ssl.SSLContext, list[str]]:
    """Create SSL context from configuration.
    
    Args:
        ssl_config: SSL configuration dictionary with keys:
            - ssl_verify_mode: CERT_NONE, CERT_OPTIONAL, or CERT_REQUIRED (default)
            - ssl_ca_secret: Airflow Variable name for CA certificate
            - ssl_cert_secret: Airflow Variable name for client certificate
            - ssl_key_secret: Airflow Variable name for client key
            - ssl_min_version: Minimum TLS version (e.g., TLSv1_2)
            
    Returns:
        Tuple of (ssl_context, temp_files_list)
    """
    ssl_context = ssl.create_default_context()
    temp_files = []
    
    # Configure verification mode
    verify_mode = ssl_config.get("ssl_verify_mode", "CERT_REQUIRED")
    if verify_mode == "CERT_NONE":
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
    elif verify_mode == "CERT_OPTIONAL":
        ssl_context.verify_mode = ssl.CERT_OPTIONAL
    else:  # CERT_REQUIRED (default)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
    
    # Load CA certificate
    if ssl_config.get("ssl_ca_secret"):
        ca_cert_content = Variable.get(ssl_config["ssl_ca_secret"], None)
        if ca_cert_content:
            ca_cert_file = tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False)
            ca_cert_file.write(ca_cert_content)
            ca_cert_file.close()
            ssl_context.load_verify_locations(cafile=ca_cert_file.name)
            temp_files.append(ca_cert_file.name)
    
    # Load client certificates
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
    
    # Configure minimum TLS version
    if ssl_config.get("ssl_min_version"):
        min_version = getattr(ssl.TLSVersion, ssl_config["ssl_min_version"], None)
        if min_version:
            ssl_context.minimum_version = min_version
    
    return ssl_context, temp_files
