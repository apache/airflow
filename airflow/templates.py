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

import jinja2.sandbox


class SandboxedEnvironment(jinja2.sandbox.SandboxedEnvironment):
    """SandboxedEnvironment for Airflow task templates."""

    def is_safe_attribute(self, obj, attr, value):
        """
        Allow access to ``_`` prefix vars (but not ``__``).

        Unlike the stock SandboxedEnvironment, we allow access to "private" attributes (ones starting with
        ``_``) whilst still blocking internal or truely private attributes (``__`` prefixed ones).
        """
        return not jinja2.sandbox.is_internal_attribute(obj, attr)
