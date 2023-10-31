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

"""
This module contains different `boto3` / `botocore` helpers for internal use within the Amazon provider.

.. warning::
    Only for internal usage, this module and all classes might be changed, renamed or removed in the future
    without any further notice.

:meta private:
"""
from __future__ import annotations

from botocore import UNSIGNED
from botocore.config import Config


def serialize_botocore_config_params(**params):
    """
    Helper function for convert internal botocore types to Python builtin types.

    :meta private:
    """
    if signature_version := params.get("signature_version"):
        if signature_version is UNSIGNED:
            params["signature_version"] = "unsigned"
    return params


def deserialize_botocore_config_params(**params):
    """
    Helper function for convert Python builtin types into botocore types.

    :meta private:
    """
    if signature_version := params.get("signature_version"):
        if isinstance(signature_version, str) and signature_version.lower() == "unsigned":
            params["signature_version"] = UNSIGNED
    return params


def build_botocore_config(**params) -> Config:
    """
    Build ``botocore.config.Config`` from the parameters.

    In additional convert Python builtin types into botocore types.

    :meta private:
    """
    return Config(**deserialize_botocore_config_params(**params))
