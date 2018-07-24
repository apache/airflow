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

import hdfs3
from hdfs3.utils import MyNone

from airflow import configuration
from airflow.hooks.base_hook import BaseHook
from airflow.utils.deprecation import RenamedClass


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
                "dfs.nameservices": "ns1",
                "dfs.ha.namenodes.ns1": "nn1,nn2",
                "dfs.namenode.rpc-address.ns1.nn1": "host1:8020",
                "dfs.namenode.rpc-address.ns1.nn2": "host2:8020",
                "dfs.namenode.http-address.ns1.nn1": "host1:50070",
                "dfs.namenode.http-address.ns1.nn2": "host2:50070"
            }
        },
        "autoconf": True,
        "token": "...",
        "ticket_cache": "..."
    }

    Here `pars` can be used to supply configuration options with the same key
    names as typically contained in the XML config files, which will take
    precedence over any parameters loaded from files. The `ha` configuration
    section can be used to supply options for using hdfs3 in high-availability
    mode (see the hdfs3 documentation for more details). The `autoconf` option
    controls whether hdfs3 uses the available hadoop config files for
    its configuration, whilst the `token` and `ticket_cache` options are
    used for configuring kerberos.

    Security modes can also be configured by defining appropriate value for
    the `hadoop.security.authentication` key in `pars`. Kerberos is used
    automatically if Airflow has been configured to use kerberos.

    :param str hdfs_conn_id: Connection ID to fetch parameters from.
    :param bool autoconf: Whether to use autoconfig to discover
        configuration options from the hdfs XML configuration files.
    """

    def __init__(self, hdfs_conn_id=None):
        super(HdfsHook, self).__init__(None)

        self.hdfs_conn_id = hdfs_conn_id
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            hdfs_params = {}

            # Configure kerberos security if used by Airflow.
            if configuration.conf.get("core", "security") == "kerberos":
                hdfs_params["hadoop.security.authentication"] = "kerberos"

            if self.hdfs_conn_id is None:
                self._conn = hdfs3.HDFileSystem(pars=hdfs_params, autoconf=True)
            else:
                conn_params = self.get_connection(self.hdfs_conn_id)
                conn_extra_params = conn_params.extra_dejson

                # Extract hadoop parameters from extra.
                hdfs_params.update(conn_extra_params.get("pars", {}))

                # Extract high-availability config if given.
                ha_params = conn_extra_params.get("ha", {})
                hdfs_params.update(ha_params.get("conf", {}))

                # Collect extra parameters to pass to kwargs. Note that we
                # avoid passing empty parameters, as hdfs3 seems to do
                # funky things with defining its own None variable.
                extra_kws = {
                    "user": conn_params.login or None,
                    "ticket_cache": conn_extra_params.get("ticket_cache"),
                    "token": conn_extra_params.get("token"),
                }
                extra_kws = {k: v for k, v in extra_kws.items() if v}

                # Build connection.
                self._conn = hdfs3.HDFileSystem(
                    host=ha_params.get("host") or conn_params.host or MyNone,
                    port=conn_params.port or MyNone,
                    pars=hdfs_params,
                    autoconf=conn_extra_params.get("autoconf", True),
                    **extra_kws
                )

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


HDFSHook = RenamedClass("HDFSHook", new_class=HdfsHook, old_module=__name__)
