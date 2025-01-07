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

import json
import logging
from contextlib import suppress
from json import JSONDecodeError

import attrs

log = logging.getLogger(__name__)


@attrs.define
class Connection:
    """
    A connection to an external data source.

    :param conn_id: The connection ID.
    :param conn_type: The connection type.
    :param description: The connection description.
    :param host: The host.
    :param login: The login.
    :param password: The password.
    :param schema: The schema.
    :param port: The port number.
    :param extra: Extra metadata. Non-standard data such as private/SSH keys can be saved here. JSON
        encoded object.
    """

    conn_id: str
    conn_type: str
    description: str | None = None
    host: str | None = None
    schema: str | None = None
    login: str | None = None
    password: str | None = None
    port: int | None = None
    extra: str | None = None

    def get_uri(self): ...

    def get_hook(self): ...

    @property
    def extra_dejson(self, nested: bool = False) -> dict:
        """
        Deserialize extra property to JSON.

        :param nested: Determines whether nested structures are also deserialized into JSON (default False).
        """
        extra_json = {}

        if self.extra:
            try:
                if nested:
                    for key, value in json.loads(self.extra).items():
                        extra_json[key] = value
                        if isinstance(value, str):
                            with suppress(JSONDecodeError):
                                extra_json[key] = json.loads(value)
                else:
                    extra_json = json.loads(self.extra)
            except JSONDecodeError:
                log.exception("Failed parsing the json for conn_id %s", self.conn_id)

            # TODO: Mask sensitive keys from this list
            # mask_secret(extra)

        return extra_json
