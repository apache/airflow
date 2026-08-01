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
Compatibility module for secrets_masker.

This module provides backward compatibility for providers that still import
from airflow.sdk.execution_time.secrets_masker. Most of the implementation
has been moved to airflow.sdk._shared.secrets_masker, with one deliberate
exception: ``mask_secret`` is re-exported from airflow.sdk.log instead.
airflow.sdk.log.mask_secret masks the secret locally AND notifies the
supervisor process; airflow.sdk._shared.secrets_masker.mask_secret only
does the former, so importing 'mask_secret' from the _shared module instead
would silently weaken masking under AIP-72 (the supervisor runs as a
separate process).
"""

from __future__ import annotations

import warnings

# Note: This import from airflow-core is ok, as this is a compatibility module which we will remove in 3.2 anyways
from airflow.utils.deprecation_tools import DeprecatedImportWarning

warnings.warn(
    "Importing from 'airflow.sdk.execution_time.secrets_masker' is deprecated and will be removed in a future "
    "version. Please use 'airflow.sdk._shared.secrets_masker' instead, EXCEPT for 'mask_secret': that should be "
    "imported from 'airflow.sdk.log' instead, since 'airflow.sdk._shared.secrets_masker.mask_secret' does not "
    "notify the supervisor process and using it will silently weaken secret masking.",
    DeprecatedImportWarning,
    stacklevel=2,
)


def __getattr__(name: str):
    """Dynamically import attributes from the shared secrets_masker location."""
    try:
        if name == "mask_secret":
            from airflow.sdk.log import mask_secret

            return mask_secret

        import airflow.sdk._shared.secrets_masker as new_module

        return getattr(new_module, name)
    except AttributeError:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
