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
from hdfs3.utils import MyNone

from airflow.hooks.hdfs_hook import HdfsHook, hdfs3, configuration


class TestHDFSHook(unittest.TestCase):
    """
    Tests for the Hdfs3Hook class.

    Note that the HDFileSystem class is mocked in most of these tests
    to avoid the requirement of having a local HDFS instance for testing.
    """

    def setUp(self):
        self._mock_fs = mock.Mock()

        self._mocked_hook = HdfsHook()
        self._mocked_hook._conn = self._mock_fs

    @mock.patch.object(hdfs3, 'HDFileSystem')
    @mock.patch.object(HdfsHook, 'get_connection')
    def test_get_conn(self, conn_mock, hdfs3_mock):
        """Tests get_conn call without ID."""

        with HdfsHook() as hook:
            hook.get_conn()

        conn_mock.assert_not_called()
        hdfs3_mock.assert_called_once_with(autoconf=True, pars={})

    @mock.patch.object(hdfs3, 'HDFileSystem')
    @mock.patch.object(HdfsHook, 'get_connection')
    def test_get_conn_no_autoconf(self, conn_mock, hdfs3_mock):
        """Tests get_conn call without ID and autoconf = False."""

        with HdfsHook(autoconf=False) as hook:
            hook.get_conn()

        conn_mock.assert_not_called()
        hdfs3_mock.assert_called_once_with(autoconf=False, pars={})

    @mock.patch.object(hdfs3, 'HDFileSystem')
    @mock.patch.object(HdfsHook, 'get_connection')
    def test_get_conn_with_conn(self, conn_mock, hdfs3_mock):
        """Tests get_conn call with specified connection."""

        conn_mock.return_value = mock.Mock(
            host='namenode',
            login='hdfs_user',
            port=8020,
            extra_dejson={'pars': {'dfs.namenode.logging.level': 'info'}})

        with HdfsHook(hdfs_conn_id='hdfs_default') as hook:
            hook.get_conn()

        conn_mock.assert_called_once_with('hdfs_default')

        hdfs3_mock.assert_called_once_with(
            host='namenode',
            port=8020,
            pars={'dfs.namenode.logging.level': 'info'},
            user='hdfs_user',
            autoconf=True)

    @mock.patch.object(hdfs3, 'HDFileSystem')
    @mock.patch.object(HdfsHook, 'get_connection')
    def test_get_conn_with_conn_ha(self, conn_mock, hdfs3_mock):
        """Tests get_conn call with connection containing HA config."""

        conn_mock.return_value = mock.Mock(
            host='namenode',
            login='hdfs_user',
            port=8020,
            extra_dejson={
                'pars': {
                    'dfs.namenode.logging.level': 'info'
                },
                "ha": {
                    "host": "ns1",
                    "conf": {
                        "dfs.nameservices": "ns1",
                        "dfs.ha.namenodes.ns1": "nn1,nn2",
                        "dfs.namenode.rpc-address.ns1.nn1": "host1:8020",
                        "dfs.namenode.rpc-address.ns1.nn2": "host2:8020",
                        "dfs.namenode.http-address.ns1.nn1": "host1:50070",
                        "dfs.namenode.http-address.ns1.nn2": "host2:50070"
                    }
                }
            })

        with HdfsHook(hdfs_conn_id='hdfs_default') as hook:
            hook.get_conn()

        conn_mock.assert_called_once_with('hdfs_default')

        hdfs3_mock.assert_called_once_with(
            host='ns1',
            port=8020,
            pars={
                'dfs.namenode.logging.level': 'info',
                'dfs.nameservices': 'ns1',
                'dfs.ha.namenodes.ns1': 'nn1,nn2',
                'dfs.namenode.rpc-address.ns1.nn1': 'host1:8020',
                'dfs.namenode.rpc-address.ns1.nn2': 'host2:8020',
                'dfs.namenode.http-address.ns1.nn1': 'host1:50070',
                'dfs.namenode.http-address.ns1.nn2': 'host2:50070'
            },
            user='hdfs_user',
            autoconf=True)

    @mock.patch.object(configuration.conf, 'get')
    @mock.patch.object(hdfs3, 'HDFileSystem')
    @mock.patch.object(HdfsHook, 'get_connection')
    def test_kerberos(self, conn_mock, hdfs3_mock, conf_mock):
        """Tests setting kerberos auth from Airflow config."""

        conn_mock.return_value = mock.Mock(
            host='namenode',
            login='hdfs_user',
            port=8020,
            extra_dejson={})

        conf_mock.return_value = 'kerberos'

        with HdfsHook(hdfs_conn_id='hdfs_default') as hook:
            hook.get_conn()

        hdfs3_mock.assert_called_once_with(
            host='namenode',
            port=8020,
            user='hdfs_user',
            pars={'hadoop.security.authentication': 'kerberos'},
            autoconf=True
        )

    @mock.patch.object(configuration.conf, 'get')
    @mock.patch.object(hdfs3, 'HDFileSystem')
    def test_kerberos_wo_conn(self, hdfs3_mock, conf_mock):
        """Tests setting kerberos auth from Airflow config without conn."""

        conf_mock.return_value = 'kerberos'

        with HdfsHook() as hook:
            hook.get_conn()

        hdfs3_mock.assert_called_once_with(
            pars={'hadoop.security.authentication': 'kerberos'},
            autoconf=True
        )

    @mock.patch.object(hdfs3, 'HDFileSystem')
    @mock.patch.object(HdfsHook, 'get_connection')
    def test_get_conn_with_empty_conn(self, conn_mock, hdfs3_mock):
        """Tests get_conn call with empty connection."""

        conn_mock.return_value = mock.Mock(
            host='',
            login='',
            port='',
            extra_dejson={})

        with HdfsHook(hdfs_conn_id='hdfs_default') as hook:
            hook.get_conn()

        conn_mock.assert_called_once_with('hdfs_default')

        hdfs3_mock.assert_called_once_with(
            host=MyNone,
            port=MyNone,
            pars={},
            autoconf=True)


if __name__ == '__main__':
    unittest.main()
