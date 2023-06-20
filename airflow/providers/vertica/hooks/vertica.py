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

from vertica_python import connect

from airflow.providers.common.sql.hooks.sql import DbApiHook


class VerticaHook(DbApiHook):
    """Interact with Vertica."""

    conn_name_attr = "vertica_conn_id"
    default_conn_name = "vertica_default"
    conn_type = "vertica"
    hook_name = "Vertica"
    supports_autocommit = True

    def get_conn(self) -> connect:
        """Return verticaql connection object."""
        conn = self.get_connection(self.vertica_conn_id)  # type: ignore
        conn_config = {
            "user": conn.login,
            "password": conn.password or "",
            "database": conn.schema,
            "host": conn.host or "localhost",
        }

        if not conn.port:
            conn_config["port"] = 5433
        else:
            conn_config["port"] = int(conn.port)
            
        if conn.extra_dejson.get("connection_load_balance", False):
            conn_config["connection_load_balance"] = bool(conn.extra_dejson["connection_load_balance"])
            
        if conn.extra_dejson.get("session_label", False):
            conn_config["session_label"] = conn.extra_dejson["session_label"]

        if conn.extra_dejson.get("backup_server_node", False):
            conn_config["backup_server_node"] = conn.extra_dejson["backup_server_node"]

        if conn.extra_dejson.get("binary_transfer", False):
            conn_config["binary_transfer"] = bool(conn.extra_dejson["binary_transfer"])

        if conn.extra_dejson.get("connection_timeout", False):
            conn_config["connection_timeout"] = int(conn.extra_dejson["connection_timeout"])

        if conn.extra_dejson.get("disable_copy_local", False):
            conn_config["disable_copy_local"] = bool(conn.extra_dejson["disable_copy_local"])

        if conn.extra_dejson.get("kerberos_host_name", False):
            conn_config["kerberos_host_name"] = conn.extra_dejson["kerberos_host_name"]

        if conn.extra_dejson.get("kerberos_service_name", False):
            conn_config["kerberos_service_name"] = conn.extra_dejson["kerberos_service_name"]

        if conn.extra_dejson.get("log_level", False):
            conn_config["log_level"] = conn.extra_dejson["log_level"]
            
        if conn.extra_dejson.get("log_path", False):
            conn_config["log_path"] = conn.extra_dejson["log_path"]

        if conn.extra_dejson.get("request_complex_types", False):
            conn_config["request_complex_types"] = bool(conn.extra_dejson["request_complex_types"])

        if conn.extra_dejson.get("use_prepared_statements", False):
            conn_config["use_prepared_statements"] = bool(conn.extra_dejson["use_prepared_statements"])

        if conn.extra_dejson.get("unicode_error", False):
            conn_config["unicode_error"] = conn.extra_dejson["unicode_error"]

        if conn.extra_dejson.get("workload", False):
            conn_config["workload"] = conn.extra_dejson["workload"]

        if conn.extra_dejson.get("ssl", False):
            conn_config["ssl"] = conn.extra_dejson["ssl"]
        
        conn = connect(**conn_config)
        return conn
