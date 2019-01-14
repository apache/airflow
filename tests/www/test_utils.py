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

import unittest
import mock
from xml.dom import minidom

from airflow.www import utils


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
        html_str = utils.generate_pages(current_page, total_pages)

        # dom parser has issues with special &laquo; and &raquo;
        html_str = html_str.replace('&laquo;', '')
        html_str = html_str.replace('&raquo;', '')
        dom = minidom.parseString(html_str)
        self.assertIsNotNone(dom)

        ulist = dom.getElementsByTagName('ul')[0]
        ulist_items = ulist.getElementsByTagName('li')
        self.assertEqual(min(window, total_pages) + extra_links, len(ulist_items))

        def get_text(nodelist):
            rc = []
            for node in nodelist:
                if node.nodeType == node.TEXT_NODE:
                    rc.append(node.data)
            return ''.join(rc)

        page_items = ulist_items[2:-2]
        mid = int(len(page_items) / 2)
        for i, item in enumerate(page_items):
            a_node = item.getElementsByTagName('a')[0]
            href_link = a_node.getAttribute('href')
            node_text = get_text(a_node.childNodes)
            if node_text == str(current_page + 1):
                if check_middle:
                    self.assertEqual(mid, i)
                self.assertEqual('javascript:void(0)', a_node.getAttribute('href'))
                self.assertIn('active', item.getAttribute('class'))
            else:
                link_str = '?page=' + str(int(node_text) - 1)
                self.assertEqual(link_str, href_link)

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
        self.assertEquals('', utils.get_params())

    def test_params_search(self):
        self.assertEqual('search=bash_',
                         utils.get_params(search='bash_'))

    def test_params_showPaused_true(self):
        """Should detect True as default for showPaused"""
        self.assertEqual('',
                         utils.get_params(showPaused=True))

    def test_params_showPaused_false(self):
        self.assertEqual('showPaused=False',
                         utils.get_params(showPaused=False))

    def test_params_all(self):
        """Should return params string ordered by param key"""
        self.assertEqual('page=3&search=bash_&showPaused=False',
                         utils.get_params(showPaused=False, page=3, search='bash_'))

    def test_open_maybe_zipped_normal_file(self):
        with mock.patch(
                'io.open', mock.mock_open(read_data="data")) as mock_file:
            utils.open_maybe_zipped('/path/to/some/file.txt')
            mock_file.assert_called_with('/path/to/some/file.txt', mode='r')

    def test_open_maybe_zipped_normal_file_with_zip_in_name(self):
        path = '/path/to/fakearchive.zip.other/file.txt'
        with mock.patch(
                'io.open', mock.mock_open(read_data="data")) as mock_file:
            utils.open_maybe_zipped(path)
            mock_file.assert_called_with(path, mode='r')

    @mock.patch("zipfile.is_zipfile")
    @mock.patch("zipfile.ZipFile")
    def test_open_maybe_zipped_archive(self, mocked_ZipFile, mocked_is_zipfile):
        mocked_is_zipfile.return_value = True
        instance = mocked_ZipFile.return_value
        instance.open.return_value = mock.mock_open(read_data="data")

        utils.open_maybe_zipped('/path/to/archive.zip/deep/path/to/file.txt')

        mocked_is_zipfile.assert_called_once()
        (args, kwargs) = mocked_is_zipfile.call_args_list[0]
        self.assertEqual('/path/to/archive.zip', args[0])

        mocked_ZipFile.assert_called_once()
        (args, kwargs) = mocked_ZipFile.call_args_list[0]
        self.assertEqual('/path/to/archive.zip', args[0])

        instance.open.assert_called_once()
        (args, kwargs) = instance.open.call_args_list[0]
        self.assertEqual('deep/path/to/file.txt', args[0])


if __name__ == '__main__':
    unittest.main()
