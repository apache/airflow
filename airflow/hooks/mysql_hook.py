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

import MySQLdb
import MySQLdb.cursors
import json

from airflow.hooks.dbapi_hook import DbApiHook


class MySqlHook(DbApiHook):
    """
    Interact with MySQL.

    You can specify charset in the extra field of your connection
    as ``{"charset": "utf8"}``. Also you can choose cursor as
    ``{"cursor": "SSCursor"}``. Refer to the MySQLdb.cursors for more details.

    Note: For AWS IAM authentication, use iam in the extra connection parameters
    and set it to true. Leave the password field empty. This will use the the
    "aws_default" connection to get the temporary token unless you override
    in extras.
    extras example: ``{"iam":true, "aws_conn_id":"my_aws_conn"}``
    """

    conn_name_attr = 'mysql_conn_id'
    default_conn_name = 'mysql_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def set_autocommit(self, conn, autocommit):
        """
        MySql connection sets autocommit in a different way.
        """
        conn.autocommit(autocommit)

    def get_autocommit(self, conn):
        """
        MySql connection gets autocommit in a different way.

        :param conn: connection to get autocommit setting from.
        :type conn: connection object.
        :return: connection autocommit setting
        :rtype: bool
        """
        return conn.get_autocommit()

    def get_conn(self):
        """
        Returns a mysql connection object
        """
        conn = self.get_connection(self.mysql_conn_id)

        conn_config = {
            "user": conn.login,
            "passwd": conn.password or '',
            "host": conn.host or 'localhost',
            "db": self.schema or conn.schema or ''
        }

        # check for authentication via AWS IAM
        if conn.extra_dejson.get('iam', False):
            conn_config['passwd'], conn.port = self.get_iam_token(conn)
            conn_config["read_default_group"] = 'enable-cleartext-plugin'

        if not conn.port:
            conn_config["port"] = 3306
        else:
            conn_config["port"] = int(conn.port)

        if conn.extra_dejson.get('charset', False):
            conn_config["charset"] = conn.extra_dejson["charset"]
            if (conn_config["charset"]).lower() == 'utf8' or\
                    (conn_config["charset"]).lower() == 'utf-8':
                conn_config["use_unicode"] = True
        if conn.extra_dejson.get('cursor', False):
            if (conn.extra_dejson["cursor"]).lower() == 'sscursor':
                conn_config["cursorclass"] = MySQLdb.cursors.SSCursor
            elif (conn.extra_dejson["cursor"]).lower() == 'dictcursor':
                conn_config["cursorclass"] = MySQLdb.cursors.DictCursor
            elif (conn.extra_dejson["cursor"]).lower() == 'ssdictcursor':
                conn_config["cursorclass"] = MySQLdb.cursors.SSDictCursor
        local_infile = conn.extra_dejson.get('local_infile', False)
        if conn.extra_dejson.get('ssl', False):
            # SSL parameter for MySQL has to be a dictionary and in case
            # of extra/dejson we can get string if extra is passed via
            # URL parameters
            dejson_ssl = conn.extra_dejson['ssl']
            if isinstance(dejson_ssl, str):
                dejson_ssl = json.loads(dejson_ssl)
            conn_config['ssl'] = dejson_ssl
        if conn.extra_dejson.get('unix_socket'):
            conn_config['unix_socket'] = conn.extra_dejson['unix_socket']
        if local_infile:
            conn_config["local_infile"] = 1
        conn = MySQLdb.connect(**conn_config)
        return conn

    def bulk_load(self, table, tmp_file):
        """
        Loads a tab-delimited file into a database table
        """
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("""
            LOAD DATA LOCAL INFILE '{tmp_file}'
            INTO TABLE {table}
            """.format(tmp_file=tmp_file, table=table))
        conn.commit()

    def bulk_dump(self, table, tmp_file):
        """
        Dumps a database table into a tab-delimited file
        """
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT * INTO OUTFILE '{tmp_file}'
            FROM {table}
            """.format(tmp_file=tmp_file, table=table))
        conn.commit()

    @staticmethod
    def _serialize_cell(cell, conn):
        """
        MySQLdb converts an argument to a literal
        when passing those separately to execute. Hence, this method does nothing.

        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The same cell
        :rtype: object
        """

        return cell

    def get_iam_token(self, conn):
        """
        Uses AWSHook to retrieve a temporary password to connect to MySQL
        Port is required. If none is provided, default 3306 is used
        """
        from airflow.contrib.hooks.aws_hook import AwsHook

        aws_conn_id = conn.extra_dejson.get('aws_conn_id', 'aws_default')
        aws_hook = AwsHook(aws_conn_id)
        if conn.port is None:
            port = 3306
        else:
            port = conn.port
        client = aws_hook.get_client_type('rds')
        token = client.generate_db_auth_token(conn.host, port, conn.login)
        return token, port
