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

from typing import TYPE_CHECKING

from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)

if TYPE_CHECKING:
    from flask_session import Session


class FakeAppBuilder:
    """
    Stand-in class to replace a Flask App Builder.

    The only purpose is to provide the ``self.appbuilder.get_session`` interface
    for ``ApplessAirflowSecurityManager`` so it can be used without a real Flask
    app, which is slow to create.
    """

    def __init__(self, session: Session | None = None) -> None:
        self.get_session = session


class ApplessAirflowSecurityManager(FabAirflowSecurityManagerOverride):
    """Security Manager that doesn't need the whole flask app."""

    def __init__(self, session: Session | None = None):
        self.appbuilder = FakeAppBuilder(session)
