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

from unittest.mock import Mock

from flask_appbuilder import BaseView

from airflow.www import app
from airflow.www.decorators import has_dag_access


class TestDecorators():

    def test_has_dag_access_redirection(self):
        class DummyView(BaseView):

            def __init__(self, appbuilder):
                BaseView.__init__(self)
                self.appbuilder = appbuilder

            @has_dag_access(can_dag_edit=True)
            def dummy_function(self):
                pass

        testing_app = app.create_app(testing=True)
        appbuilder = Mock()
        appbuilder.sm.can_edit_dag.return_value = False
        appbuilder.sm.auth_view.__class__.__name__ = 'AuthDBView'
        view = DummyView(appbuilder)

        with testing_app.test_request_context('/landingpage'):
            response = view.dummy_function()
            assert response.headers['Location'] == '/login/?next=http%3A%2F%2Flocalhost%2Flandingpage'
