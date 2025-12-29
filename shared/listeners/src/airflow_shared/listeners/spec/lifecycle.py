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

from pluggy import HookspecMarker

hookspec = HookspecMarker("airflow")


@hookspec
def on_starting(component):
    """
    Execute before Airflow component - jobs like scheduler, worker, or task runner starts.

    It's guaranteed this will be called before any other plugin method.

    :param component: Component that calls this method
    """


@hookspec
def before_stopping(component):
    """
    Execute before Airflow component - jobs like scheduler, worker, or task runner stops.

    It's guaranteed this will be called after any other plugin method.

    :param component: Component that calls this method
    """
