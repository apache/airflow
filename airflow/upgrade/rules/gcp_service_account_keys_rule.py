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


class GCPServiceAccountKeyRule(BaseRule):
    title = "GCP service account key deprecation"

    description = """Option has been removed because it is no longer \
supported by the Google Kubernetes Engine."""

    def check(self):
        gcp_option = conf.get(section="kubernetes", key="gcp_service_account_keys")
        if gcp_option:
            msg = """This option has been removed because it is no longer \
supported by the Google Kubernetes Engine. The new recommended \
service account keys for the Google Cloud management method is \
Workload Identity."""
            return [msg]
        else:
            return None
