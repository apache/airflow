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

import warnings

import hdfs3
from hdfs3.utils import MyNone

from airflow.hooks.base_hook import BaseHook


class HdfsHook(BaseHook):
    """Hook for interacting with HDFS using the hdfs3 library.

    By default hdfs3 loads its configuration from `core-site.xml` and
    `hdfs-site.xml` if these files can be found in any of the typical
    locations. The hook loads `host` and `port` parameters from the
    hdfs connection (if given) and extra configuration parameters can be
    supplied using the `pars` key in extra JSON. See the hdfs3 documentation
    for more details.

    :param str hdfs_conn_id: Connection ID to fetch parameters from.
    :param bool autoconf: Whether to use autoconfig to discover
        configuration options from the hdfs XML configuration files.
    """

    def __init__(self, hdfs_conn_id=None, autoconf=True):
        super().__init__(None)

        self.hdfs_conn_id = hdfs_conn_id
        self._autoconf = autoconf

        self._conn = None

    def get_conn(self):
        if self._conn is None:
            if self.hdfs_conn_id is None:
                self._conn = hdfs3.HDFileSystem(autoconf=self._autoconf)
            else:
                params = self.get_connection(self.hdfs_conn_id)

                # Extract hadoop parameters from extra.
                hdfs_pars = params.extra_dejson.get('pars', {})

                # Collect extra parameters to pass to kwargs.
                extra_kws = {}
                if params.login:
                    extra_kws['user'] = params.login

                # Build connection.
                self._conn = hdfs3.HDFileSystem(
                    host=params.host or MyNone,
                    port=params.port or MyNone,
                    pars=hdfs_pars,
                    autoconf=self._autoconf,
                    **extra_kws)

        return self._conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Closes the HDFSHook and any underlying connections."""

        if self._conn is not None:
            self._conn.disconnect()
        self._conn = None


class _DeprecationHelper(object):
    def __init__(self, new_target, message, category=PendingDeprecationWarning):
        self._message = message
        self._new_target = new_target
        self._category = category

    def _warn(self):
        warnings.warn(self._message, category=self._category)

    def __call__(self, *args, **kwargs):
        self._warn()
        return self._new_target(*args, **kwargs)

    def __getattr__(self, attr):
        self._warn()
        return getattr(self._new_target, attr)


HDFSHook = _DeprecationHelper(
    HdfsHook,
    message='The `HDFSHook` has been renamed to `HdfsHook`. Support for '
            'the old naming will be dropped in a future version of Airflow.')
