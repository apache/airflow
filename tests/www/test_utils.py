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

import mock
import unittest

from airflow.www import app as application

from airflow.www import utils


class UtilsTest(unittest.TestCase):

    def setUp(self):
        super(UtilsTest, self).setUp()

    # flask_login is loaded by calling flask_login._get_user.
    @mock.patch("flask_login._get_user")
    @mock.patch("airflow.settings.Session")
    def test_action_logging_with_login_user(self, mocked_session, mocked_get_user):
        fake_username = 'someone'
        mocked_current_user = mock.MagicMock()
        mocked_get_user.return_value = mocked_current_user
        mocked_current_user.user.username = fake_username
        mocked_session_instance = mock.MagicMock()
        mocked_session.return_value = mocked_session_instance

        app = application.create_app(testing=True)
        # Patching here to avoid errors in applicant.create_app
        with mock.patch("airflow.models.Log") as mocked_log:
            with app.test_request_context():
                @utils.action_logging
                def some_func():
                    pass

                some_func()
                mocked_log.assert_called_once()
                (args, kwargs) = mocked_log.call_args_list[0]
                self.assertEqual('some_func', kwargs['event'])
                self.assertEqual(fake_username, kwargs['owner'])
                mocked_session_instance.add.assert_called_once()

    @mock.patch("flask_login._get_user")
    @mock.patch("airflow.settings.Session")
    def test_action_logging_with_invalid_user(self, mocked_session, mocked_get_user):
        anonymous_username = 'anonymous'

        # When the user returned by flask login_manager._load_user
        # is invalid.
        mocked_current_user = mock.MagicMock()
        mocked_get_user.return_value = mocked_current_user
        mocked_current_user.user = None
        mocked_session_instance = mock.MagicMock()
        mocked_session.return_value = mocked_session_instance

        app = application.create_app(testing=True)
        # Patching here to avoid errors in applicant.create_app
        with mock.patch("airflow.models.Log") as mocked_log:
            with app.test_request_context():
                @utils.action_logging
                def some_func():
                    pass

                some_func()
                mocked_log.assert_called_once()
                (args, kwargs) = mocked_log.call_args_list[0]
                self.assertEqual('some_func', kwargs['event'])
                self.assertEqual(anonymous_username, kwargs['owner'])
                mocked_session_instance.add.assert_called_once()

    # flask_login.current_user would be AnonymousUserMixin
    # when there's no user_id in the flask session.
    @mock.patch("airflow.settings.Session")
    def test_action_logging_with_anonymous_user(self, mocked_session):
        anonymous_username = 'anonymous'

        mocked_session_instance = mock.MagicMock()
        mocked_session.return_value = mocked_session_instance

        app = application.create_app(testing=True)
        # Patching here to avoid errors in applicant.create_app
        with mock.patch("airflow.models.Log") as mocked_log:
            with app.test_request_context():
                @utils.action_logging
                def some_func():
                    pass

                some_func()
                mocked_log.assert_called_once()
                (args, kwargs) = mocked_log.call_args_list[0]
                self.assertEqual('some_func', kwargs['event'])
                self.assertEqual(anonymous_username, kwargs['owner'])
                mocked_session_instance.add.assert_called_once()

if __name__ == '__main__':
    unittest.main()
