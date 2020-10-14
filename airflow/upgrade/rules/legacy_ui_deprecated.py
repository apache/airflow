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

from __future__ import absolute_import

from airflow.configuration import conf
from airflow.upgrade.rules.base_rule import BaseRule

DESCRIPTION = """\
Previously we were using two versions of UI, which were hard to maintain as we
need to implement/update the same feature in both versions. With this release
we've removed the older UI in favor of Flask App Builder RBAC UI. We did it
to avoid the huge maintenance burden of two independent user interfaces.\

Before upgrading to this release, we recommend activating the new FAB RBAC UI.
For that, you should set the ``rbac`` options in ``[webserver]`` in
the airflow.cfg file to true

```ini
[webserver]
rbac = true
```
In order to login to the interface, you need to create an administrator account.

```bash
airflow create_user \\
    --role Admin \\
    --username admin \\
    --firstname FIRST_NAME \\
    --lastname LAST_NAME \\
    --email EMAIL@example.org
```
"""


class LegacyUIDeprecated(BaseRule):
    title = "Non-RBAC UI is removed in newer version"

    description = DESCRIPTION

    def check(self):
        rbac = conf.getboolean("webserver", "rbac", fallback=False)
        if not rbac:
            return "The RBAC interface is required for upgrade."
