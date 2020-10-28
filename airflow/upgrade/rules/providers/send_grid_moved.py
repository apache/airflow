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


class SendGridEmailerMovedRule(BaseRule):
    title = "SendGrid email uses old airflow.contrib module"

    description = """
The SendGrid module `airflow.contrib.utils.sendgrid` was moved to `airflow.providers.sendgrid.utils.emailer`.
    """

    def check(self):
        email_conf = conf.get(section="email", key="email_backend")
        email_contrib_path = "airflow.contrib.utils.sendgrid"
        if email_contrib_path in email_conf:
            email_provider_path = "airflow.providers.sendgrid.utils.emailer"
            msg = "Email backend option uses airflow.contrib Sendgrid module. " \
                  + "Please use new module: {}".format(email_provider_path)
            return [msg]
