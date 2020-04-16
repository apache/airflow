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

import functools

from bs4 import BeautifulSoup
import mock
import six
from six.moves.urllib.parse import parse_qs, quote_plus
from parameterized import parameterized

from airflow.models import DagRun, Log, DagBag
from airflow.settings import Session
from airflow.utils.state import State
from airflow.utils import dates, timezone
from airflow.www import utils, app as application
from tests.test_utils.config import conf_vars

if six.PY2:
    # Need `assertRegex` back-ported from unittest2
    import unittest2 as unittest
else:
    import unittest


class UtilsTest(unittest.TestCase):

    def setUp(self):
        super(UtilsTest, self).setUp()

    def test_empty_variable_should_not_be_hidden(self):
        self.assertFalse(utils.should_hide_value_for_key(""))
        self.assertFalse(utils.should_hide_value_for_key(None))

    def test_normal_variable_should_not_be_hidden(self):
        self.assertFalse(utils.should_hide_value_for_key("key"))

    def test_sensitive_variable_should_be_hidden(self):
        self.assertTrue(utils.should_hide_value_for_key("google_api_key"))

    def test_sensitive_variable_should_be_hidden_ic(self):
        self.assertTrue(utils.should_hide_value_for_key("GOOGLE_API_KEY"))

    def check_generate_pages_html(self, current_page, total_pages,
                                  window=7, check_middle=False):
        extra_links = 4  # first, prev, next, last
        search = "'>\"/><img src=x onerror=alert(1)>"
        html_str = utils.generate_pages(current_page, total_pages,
                                        search=search)

        self.assertNotIn(search, html_str,
                         "The raw search string shouldn't appear in the output")
        self.assertIn('search=%27%3E%22%2F%3E%3Cimg+src%3Dx+onerror%3Dalert%281%29%3E',
                      html_str)

        self.assertTrue(
            callable(html_str.__html__),
            "Should return something that is HTML-escaping aware"
        )

        dom = BeautifulSoup(html_str, 'html.parser')
        self.assertIsNotNone(dom)

        ulist = dom.ul
        ulist_items = ulist.find_all('li')
        self.assertEqual(min(window, total_pages) + extra_links, len(ulist_items))

        page_items = ulist_items[2:-2]
        mid = int(len(page_items) / 2)
        for i, item in enumerate(page_items):
            a_node = item.a
            href_link = a_node['href']
            node_text = a_node.string
            if node_text == str(current_page + 1):
                if check_middle:
                    self.assertEqual(mid, i)
                self.assertEqual('javascript:void(0)', href_link)
                self.assertIn('active', item['class'])
            else:
                self.assertRegex(href_link, r'^\?', 'Link is page-relative')
                query = parse_qs(href_link[1:])
                self.assertListEqual(query['page'], [str(int(node_text) - 1)])
                self.assertListEqual(query['search'], [search])

    def test_generate_pager_current_start(self):
        self.check_generate_pages_html(current_page=0,
                                       total_pages=6)

    def test_generate_pager_current_middle(self):
        self.check_generate_pages_html(current_page=10,
                                       total_pages=20,
                                       check_middle=True)

    def test_generate_pager_current_end(self):
        self.check_generate_pages_html(current_page=38,
                                       total_pages=39)

    def test_params_no_values(self):
        """Should return an empty string if no params are passed"""
        self.assertEqual('', utils.get_params())

    def test_params_search(self):
        self.assertEqual('search=bash_',
                         utils.get_params(search='bash_'))

    @parameterized.expand([
        (True, False, ''),
        (False, True, ''),
        (True, True, 'showPaused=True'),
        (False, False, 'showPaused=False'),
        (None, True, ''),
        (None, False, ''),
    ])
    def test_params_showPaused(self, show_paused, hide_by_default, expected_result):
        with conf_vars({('webserver', 'hide_paused_dags_by_default'): str(hide_by_default)}):
            self.assertEqual(expected_result,
                             utils.get_params(showPaused=show_paused))

    @parameterized.expand([
        (True, False, True),
        (False, True, True),
        (True, True, False),
        (False, False, False),
        (None, True, True),
        (None, False, True),
    ])
    def test_should_remove_show_paused_from_url_params(self, show_paused,
                                                       hide_by_default, expected_result):
        with conf_vars({('webserver', 'hide_paused_dags_by_default'): str(hide_by_default)}):

            self.assertEqual(
                expected_result,
                utils._should_remove_show_paused_from_url_params(
                    show_paused,
                    hide_by_default
                )
            )

    def test_params_none_and_zero(self):
        qs = utils.get_params(a=0, b=None)
        # The order won't be consistent, but that doesn't affect behaviour of a browser
        pairs = list(sorted(qs.split('&')))
        self.assertListEqual(['a=0', 'b='], pairs)

    def test_params_all(self):
        query = utils.get_params(showPaused=False, page=3, search='bash_')
        self.assertEqual(
            {'page': ['3'],
             'search': ['bash_'],
             'showPaused': ['False']},
            parse_qs(query)
        )

    def test_params_escape(self):
        self.assertEqual('search=%27%3E%22%2F%3E%3Cimg+src%3Dx+onerror%3Dalert%281%29%3E',
                         utils.get_params(search="'>\"/><img src=x onerror=alert(1)>"))

    # flask_login is loaded by calling flask_login.utils._get_user.
    @mock.patch("flask_login.utils._get_user")
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

    @mock.patch("flask_login.utils._get_user")
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

    def test_get_python_source_from_method(self):
        class AMockClass(object):
            def a_method(self):
                """ A method """
                pass

        mocked_class = AMockClass()

        result = utils.get_python_source(mocked_class.a_method)
        self.assertIn('A method', result)

    def test_get_python_source_from_class(self):
        class AMockClass(object):
            def __call__(self):
                """ A __call__ method """
                pass

        mocked_class = AMockClass()

        result = utils.get_python_source(mocked_class)
        self.assertIn('A __call__ method', result)

    def test_get_python_source_from_partial_func(self):
        def a_function(arg_x, arg_y):
            """ A function with two args """
            pass

        partial_function = functools.partial(a_function, arg_x=1)

        result = utils.get_python_source(partial_function)
        self.assertIn('A function with two args', result)

    def test_get_python_source_from_none(self):
        result = utils.get_python_source(None)
        self.assertIn('No source code available', result)


class TestDecorators(unittest.TestCase):
    EXAMPLE_DAG_DEFAULT_DATE = dates.days_ago(2)
    run_id = "test_{}".format(DagRun.id_for_date(EXAMPLE_DAG_DEFAULT_DATE))

    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(include_examples=True)
        app = application.create_app(testing=True)
        app.config['WTF_CSRF_METHODS'] = []
        cls.app = app.test_client()

    def setUp(self):
        self.session = Session()
        self.cleanup_dagruns()
        self.prepare_dagruns()

    def cleanup_dagruns(self):
        DR = DagRun
        dag_ids = 'example_bash_operator'
        (self.session
             .query(DR)
             .filter(DR.dag_id == dag_ids)
             .filter(DR.run_id == self.run_id)
             .delete(synchronize_session='fetch'))
        self.session.commit()

    def prepare_dagruns(self):
        self.bash_dag = self.dagbag.dags['example_bash_operator']
        self.bash_dag.sync_to_db()

        self.bash_dagrun = self.bash_dag.create_dagrun(
            run_id=self.run_id,
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.RUNNING)

    def check_last_log(self, dag_id, event, execution_date=None):
        qry = self.session.query(Log.dag_id, Log.task_id, Log.event, Log.execution_date,
                                 Log.owner, Log.extra)
        qry = qry.filter(Log.dag_id == dag_id, Log.event == event)
        if execution_date:
            qry = qry.filter(Log.execution_date == execution_date)
        logs = qry.order_by(Log.dttm.desc()).limit(5).all()
        self.assertGreaterEqual(len(logs), 1)
        self.assertTrue(logs[0].extra)

    def test_action_logging_get(self):
        url = '/admin/airflow/graph?dag_id=example_bash_operator&execution_date={}'.format(
            quote_plus(self.EXAMPLE_DAG_DEFAULT_DATE.isoformat().encode('utf-8')))
        self.app.get(url, follow_redirects=True)

        # In mysql backend, this commit() is needed to write down the logs
        self.session.commit()
        self.check_last_log("example_bash_operator", event="graph",
                            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE)

    def test_action_logging_post(self):
        form = dict(
            task_id="runme_1",
            dag_id="example_bash_operator",
            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE.isoformat().encode('utf-8'),
            upstream="false",
            downstream="false",
            future="false",
            past="false",
            only_failed="false",
        )
        self.app.post("/admin/airflow/clear", data=form)
        # In mysql backend, this commit() is needed to write down the logs
        self.session.commit()
        self.check_last_log("example_bash_operator", event="clear",
                            execution_date=self.EXAMPLE_DAG_DEFAULT_DATE)


if __name__ == '__main__':
    unittest.main()
