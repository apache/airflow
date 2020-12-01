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


class HostnameCallable(BaseRule):
    title = "Unify hostname_callable option in core section"

    description = ""

    def check(self):
        hostname_callable_conf = conf.get("core", "hostname_callable")
        if ":" in hostname_callable_conf:
            return (
                "Error: hostname_callable `{}` "
                "contains a colon instead of a dot. please change to `{}`".format(
                    hostname_callable_conf, hostname_callable_conf.replace(":", ".")
                )
            )
        return None
