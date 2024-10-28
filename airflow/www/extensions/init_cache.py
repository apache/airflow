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

import hashlib
from tempfile import gettempdir

from flask_caching import Cache

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException

HASH_METHOD_MAPPING = {
    "md5": hashlib.md5,
    "sha1": hashlib.sha1,
    "sha224": hashlib.sha224,
    "sha256": hashlib.sha256,
    "sha384": hashlib.sha384,
    "sha512": hashlib.sha512,
}


def init_cache(app):
    webserver_caching_hash_method = conf.get(
        section="webserver", key="CACHING_HASH_METHOD", fallback="md5"
    ).casefold()
    cache_config = {
        "CACHE_TYPE": "flask_caching.backends.filesystem",
        "CACHE_DIR": gettempdir(),
    }

    mapped_hash_method = HASH_METHOD_MAPPING.get(webserver_caching_hash_method)

    if mapped_hash_method is None:
        raise AirflowConfigException(
            f"Unsupported webserver caching hash method: `{webserver_caching_hash_method}`."
        )

    cache_config["CACHE_OPTIONS"] = {"hash_method": mapped_hash_method}

    Cache(app=app, config=cache_config)
