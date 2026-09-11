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

from unittest import mock

import pytest
from flask import Flask

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.fab.www.extensions.init_security import init_api_auth

from tests_common.test_utils.config import conf_vars

IMPORT_MODULE = "airflow.providers.fab.www.extensions.init_security.import_module"
SESSION_BACKEND = "airflow.providers.fab.auth_manager.api.auth.backend.session"


class TestInitApiAuth:
    @conf_vars({("fab", "auth_backends"): "first.backend, second.backend"})
    def test_every_configured_backend_is_imported_and_initialised_in_order(self):
        flask_app = Flask(__name__)
        imported: dict[str, mock.MagicMock] = {}

        def fake_import(name):
            return imported.setdefault(name, mock.MagicMock())

        with mock.patch(IMPORT_MODULE, side_effect=fake_import) as import_module:
            init_api_auth(flask_app)

        # The whitespace that follows the comma has to be stripped before importing.
        assert import_module.call_args_list == [
            mock.call("first.backend"),
            mock.call("second.backend"),
        ]
        backends = list(imported.values())
        for backend in backends:
            backend.init_app.assert_called_once_with(flask_app)
        assert flask_app.api_auth == backends

    @conf_vars({("fab", "auth_backends"): None})
    def test_the_session_backend_is_used_when_nothing_is_configured(self):
        flask_app = Flask(__name__)

        with mock.patch(IMPORT_MODULE) as import_module:
            init_api_auth(flask_app)

        import_module.assert_called_once_with(SESSION_BACKEND)

    @conf_vars({("fab", "auth_backends"): "missing.backend"})
    def test_an_unimportable_backend_is_reported_as_an_airflow_exception(self):
        flask_app = Flask(__name__)

        with mock.patch(IMPORT_MODULE, side_effect=ImportError("boom")):
            with pytest.raises(AirflowException, match="boom"):
                init_api_auth(flask_app)

        # The first import failed, so no backend was appended.
        assert flask_app.api_auth == []

    @conf_vars({("fab", "auth_backends"): ""})
    def test_an_empty_backend_list_escapes_as_a_value_error(self):
        flask_app = Flask(__name__)

        # ``"".split(",")`` yields ``[""]``, and ``import_module("")`` raises ValueError,
        # which the ``except ImportError`` in ``init_api_auth`` does not catch. So an empty
        # ``[fab] auth_backends`` surfaces as a bare ValueError rather than the
        # AirflowException the function raises for every other bad backend.
        with pytest.raises(ValueError, match="Empty module name"):
            init_api_auth(flask_app)
