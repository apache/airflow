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

from urllib.parse import quote, urlparse, urlunparse


def fix_account_name(name: str) -> str:
    """Fix account name to have the following format: <account_id>.<region>.<cloud>."""
    spl = name.split(".")
    if len(spl) == 1:
        account = spl[0]
        region, cloud = "us-west-1", "aws"
    elif len(spl) == 2:
        account, region = spl
        cloud = "aws"
    else:
        account, region, cloud = spl
    return f"{account}.{region}.{cloud}"


def fix_snowflake_sqlalchemy_uri(uri: str) -> str:
    """
    Fix snowflake sqlalchemy connection URI to OpenLineage structure.

    Snowflake sqlalchemy connection URI has following structure:
    'snowflake://<user_login_name>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>'
    We want account identifier normalized. It can have two forms:
    - newer, in form of <organization>-<id>. In this case we want to do nothing.
    - older, composed of <id>-<region>-<cloud> where region and cloud can be
    optional in some cases. If <cloud> is omitted, it's AWS.
    If region and cloud are omitted, it's AWS us-west-1
    """
    try:
        parts = urlparse(uri)
    except ValueError:
        # snowflake.sqlalchemy.URL does not quote `[` and `]`
        # that's a rare case so we can run more debugging code here
        # to make sure we replace only password
        parts = urlparse(uri.replace("[", quote("[")).replace("]", quote("]")))

    hostname = parts.hostname
    if not hostname:
        return uri

    # old account identifier like xy123456
    if "." in hostname or not any(word in hostname for word in ["-", "_"]):
        hostname = fix_account_name(hostname)
    # else - its new hostname, just return it
    return urlunparse((parts.scheme, hostname, parts.path, parts.params, parts.query, parts.fragment))
