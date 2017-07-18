# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import unittest

from airflow import configuration
from airflow.security.kerberos import renew_from_kt


@unittest.skipIf('KRB5_KTNAME' not in os.environ,
                 'Skipping Kerberos API tests due to missing KRB5_KTNAME')
class KerberosTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

        if not configuration.conf.has_section("kerberos"):
            configuration.conf.add_section("kerberos")

        configuration.conf.set("kerberos",
                               "keytab",
                               os.environ['KRB5_KTNAME'])

    def test_renew_from_kt(self):
        """
        We expect no result, but a successful run. No more TypeError
        """
        self.assertIsNone(renew_from_kt())
