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

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from botocore.credentials import ReadOnlyCredentials

log = logging.getLogger(__name__)


def build_credentials_block(credentials: ReadOnlyCredentials) -> str:
    """
    Generate AWS credentials block for Redshift COPY and UNLOAD commands.

    See AWS docs for details:
    https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html#copy-credentials

    :param credentials: ReadOnlyCredentials object from `botocore`
    """
    if credentials.token:
        log.debug("STS token found in credentials, including it in the command")
        # these credentials are obtained from AWS STS
        # so the token must be included in the CREDENTIALS clause
        credentials_line = (
            f"aws_access_key_id={credentials.access_key};"
            f"aws_secret_access_key={credentials.secret_key};"
            f"token={credentials.token}"
        )

    else:
        credentials_line = (
            f"aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key}"
        )

    return credentials_line
