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

from airflow import configuration
from airflow.hooks.base_hook import BaseHook


class HdfsHook(BaseHook):
    """Hook for interacting with HDFS using the hdfs3 library.

    By default hdfs3 loads its configuration from `core-site.xml` and
    `hdfs-site.xml` if these files can be found in any of the typical
    locations. The hook loads `host` and `port` parameters from the
    hdfs connection (if given) and extra configuration parameters can be
    supplied in the connections extra JSON as follows:

    {
        "pars": {
            "dfs.domain.socket.path": "/var/lib/hadoop-hdfs/dn_socket"
        },
        "ha": {
            "host": "nameservice1",
            "conf": {
                "dfs.nameservices": "nameservice1",
                "dfs.ha.namenodes.nameservice1": "namenode113,namenode188",
                "dfs.namenode.rpc-address.nameservice1.namenode113": "host1:8020",
                "dfs.namenode.rpc-address.nameservice1.namenode188": "host2:8020",
                "dfs.namenode.http-address.nameservice1.namenode113": "host1:50070",
                "dfs.namenode.http-address.nameservice1.namenode188": "host2:50070"
            }
        }
    }

    Here `pars` can be used to supply configuration options with the same key
    names as typically contained in the XML config files, which will take
    precedence over any parameters loaded from files. The `ha` configuration
    section can be used to supply options for using hdfs3 in high-availability
    mode. See the hdfs3 documentation for more details.

    Security modes can also be configured by defining appropriate value for
    the `hadoop.security.authentication` key in `pars`. Kerberos is used
    automatically if Airflow has been configured to use kerberos.

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
                extra_params = params.extra_dejson

                # Extract hadoop parameters from extra.
                hdfs_params = extra_params.get("pars", {})

                # Configure kerberos security if used by Airflow.
                if configuration.conf.get("core", "security") == "kerberos":
                    hdfs_params["hadoop.security.authentication"] = "kerberos"

                # Extract high-availability config if given.
                ha_params = extra_params.get("ha", {})
                hdfs_params.update(ha_params.get("conf", {}))

                # Collect extra parameters to pass to kwargs.
                extra_kws = {}
                if params.login:
                    extra_kws["user"] = params.login

                # Build connection.
                self._conn = hdfs3.HDFileSystem(
                    host=ha_params.get("host") or params.host or MyNone,
                    port=params.port or MyNone,
                    pars=hdfs_params,
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
    message="The `HDFSHook` has been renamed to `HdfsHook`. Support for "
            "the old naming will be dropped in a future version of Airflow.")
