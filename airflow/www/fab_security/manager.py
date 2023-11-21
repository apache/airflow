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

# mypy: disable-error-code=var-annotated
from __future__ import annotations

import logging

# This product contains a modified portion of 'Flask App Builder' developed by Daniel Vaz Gaspar.
# (https://github.com/dpgaspar/Flask-AppBuilder).
# Copyright 2013, Daniel Vaz Gaspar
log = logging.getLogger(__name__)

__lazy_imports = {
    "AUTH_DB": "flask_appbuilder.const",
    "AUTH_LDAP": "flask_appbuilder.const",
    "LOGMSG_WAR_SEC_LOGIN_FAILED": "flask_appbuilder.const",
}


def __getattr__(name: str):
    # PEP-562: Lazy loaded attributes on python modules
    path = __lazy_imports.get(name)
    if not path:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from airflow.utils.module_loading import import_string

    val = import_string(f"{path}.{name}")
    # Store for next time
    globals()[name] = val
    return val
