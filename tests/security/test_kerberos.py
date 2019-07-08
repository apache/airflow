# -*- coding: utf-8 -*-
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

import os
import unittest
from argparse import Namespace
from airflow import configuration
from airflow.security.kerberos import renew_from_kt
from airflow import LoggingMixin


@unittest.skipIf('KRB5_KTNAME' not in os.environ,
                 'Skipping Kerberos API tests due to missing KRB5_KTNAME')
class KerberosTest(unittest.TestCase):
    def setUp(self):
        if not configuration.conf.has_section("kerberos"):
            configuration.conf.add_section("kerberos")
        configuration.conf.set("kerberos", "keytab",
                               os.environ['KRB5_KTNAME'])
        keytab_from_cfg = configuration.conf.get("kerberos", "keytab")
        self.args = Namespace(keytab=keytab_from_cfg, principal=None, pid=None,
                              daemon=None, stdout=None, stderr=None, log_file=None)

    def test_renew_from_kt(self):
        """
        We expect no result, but a successful run. No more TypeError
        """
        self.assertIsNone(renew_from_kt(principal=self.args.principal,
                                        keytab=self.args.keytab))

    def test_args_from_cli(self):
        """
        We expect no result, but a run with sys.exit(1) because keytab not exist.
        """
        configuration.conf.set("kerberos", "keytab", "")
        self.args.keytab = "test_keytab"

        with self.assertRaises(SystemExit) as se:
            renew_from_kt(principal=self.args.principal,
                          keytab=self.args.keytab)

            with self.assertLogs(LoggingMixin().log) as log:
                self.assertIn(
                    'kinit: krb5_init_creds_set_keytab: Failed to find '
                    'airflow@LUPUS.GRIDDYNAMICS.NET in keytab FILE:{} '
                    '(unknown enctype)'.format(self.args.keytab), log.output)

        self.assertEqual(se.exception.code, 1)
