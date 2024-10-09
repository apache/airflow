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

import logging
import os
from contextlib import nullcontext
from unittest import mock

import pytest

from airflow.security import kerberos
from airflow.security.kerberos import renew_from_kt

from dev.tests_common.test_utils.config import conf_vars


@pytest.mark.integration("kerberos")
class TestKerberosIntegration:
    @classmethod
    def setup_class(cls):
        assert "KRB5_KTNAME" in os.environ, "Missing KRB5_KTNAME environment variable"
        cls.keytab = os.environ["KRB5_KTNAME"]

    @pytest.mark.parametrize(
        "kerberos_config",
        [
            pytest.param({}, id="default-config"),
            pytest.param({("kerberos", "include_ip"): "True"}, id="explicit-include-ip"),
            pytest.param({("kerberos", "include_ip"): "False"}, id="explicit-not-include-ip"),
            pytest.param({("kerberos", "forwardable"): "True"}, id="explicit-forwardable"),
            pytest.param({("kerberos", "forwardable"): "False"}, id="explicit-not-forwardable"),
        ],
    )
    def test_renew_from_kt(self, kerberos_config):
        """We expect return 0 (exit code) and successful run."""
        with conf_vars(kerberos_config):
            assert renew_from_kt(principal=None, keytab=self.keytab) == 0

    @pytest.mark.parametrize(
        "exit_on_fail, expected_context",
        [
            pytest.param(True, pytest.raises(SystemExit), id="exit-on-fail"),
            pytest.param(False, nullcontext(), id="return-code-of-fail"),
        ],
    )
    def test_args_from_cli(self, exit_on_fail, expected_context, caplog):
        """Test exit code if keytab not exist."""
        keytab = "/not/exists/keytab"
        result = None

        with mock.patch.dict(os.environ, KRB5_KTNAME=keytab), conf_vars({("kerberos", "keytab"): keytab}):
            with expected_context as ctx:
                with caplog.at_level(logging.ERROR, logger=kerberos.log.name):
                    caplog.clear()
                    result = renew_from_kt(principal=None, keytab=keytab, exit_on_fail=exit_on_fail)

        # If `exit_on_fail` set to True than exit code in exception, otherwise in function return
        exit_code = ctx.value.code if exit_on_fail else result
        assert exit_code == 1
        assert caplog.record_tuples
