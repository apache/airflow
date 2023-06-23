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
            
        conn_extra = conn.extra_dejson        
        if "connection_load_balance" in conn_extra:
            conn_config["connection_load_balance"] = bool(conn_extra["connection_load_balance"])
            
        conn_config["session_label"] = conn_extra.get("session_label")
        conn_config["backup_server_node"] = conn_extra.get("backup_server_node")

        if "binary_transfer" in conn_extra:
            conn_config["binary_transfer"] = bool(conn_extra["binary_transfer"])

        if "connection_timeout" in conn_extra:
            conn_config["connection_timeout"] = int(conn_extra["connection_timeout"])

        if "disable_copy_local" in conn_extra:
            conn_config["disable_copy_local"] = bool(conn_extra["disable_copy_local"])

        conn_config["kerberos_host_name"] = conn_extra.get("kerberos_host_name")
        conn_config["kerberos_service_name"] = conn_extra.get("kerberos_service_name")

        if "log_level" in conn_extra:
            import logging
            log_lvl = conn_extra["log_level"]
            if isinstance(log_lvl, str):
                log_lvl = log_lvl.lower()
                if log_lvl == "critical":
                    conn_config["log_level"] = logging.CRITICAL
                elif log_lvl == "error":
                    conn_config["log_level"] = logging.ERROR
                elif log_lvl == "warning":
                    conn_config["log_level"] = logging.WARNING
                elif log_lvl == "info":
                    conn_config["log_level"] = logging.INFO
                elif log_lvl == "debug":
                    conn_config["log_level"] = logging.DEBUG
                elif log_lvl == "notset":
                    conn_config["log_level"] = logging.NOTSET
            else:
                conn_config["log_level"] = int(conn_extra["log_level"])
            
        if "log_path" in conn_extra:
            conn_config["log_path"] = conn_extra["log_path"]

        if "request_complex_types" in conn_extra:
            conn_config["request_complex_types"] = bool(conn_extra["request_complex_types"])

        if "use_prepared_statements" in conn_extra:
            conn_config["use_prepared_statements"] = bool(conn_extra["use_prepared_statements"])

        conn_config["unicode_error"] = conn_extra.get("unicode_error")
        conn_config["workload"] = conn_extra.get("workload")
        conn_config["ssl"] = conn_extra.get("ssl")
        
        conn = connect(**conn_config)
        return conn
