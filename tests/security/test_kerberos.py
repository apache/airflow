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

from airflow.security import kerberos
from airflow.security.kerberos import renew_from_kt
from tests.test_utils.config import conf_vars

KRB5_KTNAME = os.environ.get('KRB5_KTNAME')


@unittest.skipIf(KRB5_KTNAME is None, 'Skipping Kerberos API tests due to missing KRB5_KTNAME')
class TestKerberos(unittest.TestCase):
    def setUp(self):
        self.args = Namespace(
            keytab=KRB5_KTNAME, principal=None, pid=None, daemon=None, stdout=None, stderr=None, log_file=None
        )

    @conf_vars({('kerberos', 'keytab'): KRB5_KTNAME})
    def test_renew_from_kt(self):
        """
        We expect no result, but a successful run. No more TypeError
        """
        self.assertIsNone(
            renew_from_kt(principal=self.args.principal, keytab=self.args.keytab)  # pylint: disable=no-member
        )

    @conf_vars({('kerberos', 'keytab'): ''})
    def test_args_from_cli(self):
        """
        We expect no result, but a run with sys.exit(1) because keytab not exist.
        """
        self.args.keytab = "test_keytab"

        with self.assertRaises(SystemExit) as err:
            renew_from_kt(principal=self.args.principal, keytab=self.args.keytab)  # pylint: disable=no-member

            with self.assertLogs(kerberos.log) as log:
                self.assertIn(
                    'kinit: krb5_init_creds_set_keytab: Failed to find '
                    'airflow@LUPUS.GRIDDYNAMICS.NET in keytab FILE:{} '
                    '(unknown enctype)'.format(self.args.keytab),
                    log.output,
                )

            self.assertEqual(err.exception.code, 1)
