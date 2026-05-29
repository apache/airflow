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

import warnings

from fastapi import HTTPException, status
from fastapi.responses import Response

from airflow.api_fastapi.common.types import Mimetype
from airflow.api_fastapi.core_api.datamodels.config import Config
from airflow.configuration import conf

# Per-key environment-variable overrides for secrets-backend kwargs are
# surfaced by ``conf.as_dict`` as synthetic options under the ``secrets``
# and ``workers`` sections. They carry the same secrets-backend material
# (e.g. Vault role_id / secret_id) as the registered ``backend_kwargs``
# option, so they need the same redaction treatment when
# ``display_sensitive=False``.
_PER_KEY_SENSITIVE_PREFIXES: dict[str, str] = {
    "secrets": "backend_kwarg__",
    "workers": "secrets_backend_kwarg__",
}


def _is_per_key_sensitive_option(section: str, option: str) -> bool:
    """Return True for synthetic per-key secrets-backend-kwarg options."""
    prefix = _PER_KEY_SENSITIVE_PREFIXES.get(section)
    return prefix is not None and option.startswith(prefix)


def _mask_per_key_sensitive_options(conf_dict: dict) -> None:
    """Mask synthetic per-key secrets-backend-kwarg options in-place."""
    for section, prefix in _PER_KEY_SENSITIVE_PREFIXES.items():
        options = conf_dict.get(section)
        if not options:
            continue
        for option in list(options):
            if option.startswith(prefix):
                current = options[option]
                if isinstance(current, tuple):
                    options[option] = ("< hidden >", current[1])
                else:
                    options[option] = "< hidden >"


def _check_expose_config() -> bool:
    display_sensitive: bool | None = None
    if conf.get("api", "expose_config").lower() == "non-sensitive-only":
        expose_config = True
        display_sensitive = False
        warnings.warn(
            "The value 'non-sensitive-only' for [api] expose_config is deprecated. "
            "Use 'true' instead; sensitive configuration values are always masked.",
            DeprecationWarning,
            stacklevel=2,
        )
    else:
        expose_config = conf.getboolean("api", "expose_config")
        display_sensitive = False

    if not expose_config:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your Airflow administrator chose not to expose the configuration, most likely for security reasons.",
        )
    return display_sensitive


def _response_based_on_accept(accept: Mimetype, config: Config):
    if accept == Mimetype.TEXT:
        return Response(content=config.text_format, media_type=Mimetype.TEXT)
    return config
