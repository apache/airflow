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

'''Plugins metabrowser'''

import json
from datetime import datetime
from typing import List

import pandas as pd
from flask import Blueprint, Markup, request
from flask_appbuilder import BaseView, expose

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.apache.hive.hooks.hive import HiveCliHook, HiveMetastoreHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.presto.hooks.presto import PrestoHook
from airflow.www.decorators import gzipped

METASTORE_CONN_ID = 'metastore_default'
METASTORE_MYSQL_CONN_ID = 'metastore_mysql'
PRESTO_CONN_ID = 'presto_default'
HIVE_CLI_CONN_ID = 'hive_default'
DEFAULT_DB = 'default'
DB_ALLOW_LIST = []  # type: List[str]
DB_DENY_LIST = ['tmp']  # type: List[str]
TABLE_SELECTOR_LIMIT = 2000

# Keeping pandas from truncating long strings
pd.set_option('display.max_colwidth', -1)


class MetastoreBrowserView(BaseView):
    """
    Creating a Flask-AppBuilder BaseView
    """

    default_view = 'index'

    @expose('/')
    def index(self):
        """
        Create default view
        """
        sql = """
        SELECT
            a.name as db, db_location_uri as location,
            count(1) as object_count, a.desc as description
        FROM DBS a
        JOIN TBLS b ON a.DB_ID = b.DB_ID
        GROUP BY a.name, db_location_uri, a.desc
        """
        hook = MySqlHook(METASTORE_MYSQL_CONN_ID)
        df = hook.get_pandas_df(sql)
        df.db = (
            '<a href="/metastorebrowserview/db/?db=' +
            df.db + '">' + df.db + '</a>')
        table = df.to_html(
            classes="table table-striped table-bordered table-hover",
            index=False,
            escape=False,
            na_rep='',)
        return self.render_template(
            "metastore_browser/dbs.html", table=Markup(table))

    @expose('/table/')
    def table(self):
        """
        Create table view
        """
        table_name = request.args.get("table")
        metastore = HiveMetastoreHook(METASTORE_CONN_ID)
        table = metastore.get_table(table_name)
        return self.render_template(
            "metastore_browser/table.html",
            table=table, table_name=table_name, datetime=datetime, int=int)

    @expose('/db/')
    def db(self):
        """
        Show tables in database
        """
        db = request.args.get("db")
        metastore = HiveMetastoreHook(METASTORE_CONN_ID)
        tables = sorted(metastore.get_tables(db=db), key=lambda x: x.tableName)
        return self.render_template(
            "metastore_browser/db.html", tables=tables, db=db)

    @gzipped
    @expose('/partitions/')
    def partitions(self):
        """
        Retrieve table partitions
        """
        schema, table = request.args.get("table").split('.')
        sql = """
        SELECT
            a.PART_NAME,
            a.CREATE_TIME,
            c.LOCATION,
            c.IS_COMPRESSED,
            c.INPUT_FORMAT,
            c.OUTPUT_FORMAT
        FROM PARTITIONS a
        JOIN TBLS b ON a.TBL_ID = b.TBL_ID
        JOIN DBS d ON b.DB_ID = d.DB_ID
        JOIN SDS c ON a.SD_ID = c.SD_ID
        WHERE
            b.TBL_NAME like '{table}' AND
            d.NAME like '{schema}'
        ORDER BY PART_NAME DESC
        """.format(table=table, schema=schema)
        hook = MySqlHook(METASTORE_MYSQL_CONN_ID)
        df = hook.get_pandas_df(sql)
        return df.to_html(
            classes="table table-striped table-bordered table-hover",
            index=False,
            na_rep='',)

    @gzipped
    @expose('/objects/')
    def objects(self):
        """
        Retrieve objects from TBLS and DBS
        """
        where_clause = ''
        if DB_ALLOW_LIST:
            dbs = ",".join(["'" + db + "'" for db in DB_ALLOW_LIST])
            where_clause = "AND b.name IN ({})".format(dbs)
        if DB_DENY_LIST:
            dbs = ",".join(["'" + db + "'" for db in DB_DENY_LIST])
            where_clause = "AND b.name NOT IN ({})".format(dbs)
        sql = """
        SELECT CONCAT(b.NAME, '.', a.TBL_NAME), TBL_TYPE
        FROM TBLS a
        JOIN DBS b ON a.DB_ID = b.DB_ID
        WHERE
            a.TBL_NAME NOT LIKE '%tmp%' AND
            a.TBL_NAME NOT LIKE '%temp%' AND
            b.NAME NOT LIKE '%tmp%' AND
            b.NAME NOT LIKE '%temp%'
        {where_clause}
        LIMIT {LIMIT};
        """.format(where_clause=where_clause, LIMIT=TABLE_SELECTOR_LIMIT)
        hook = MySqlHook(METASTORE_MYSQL_CONN_ID)
        data = [
            {'id': row[0], 'text': row[0]}
            for row in hook.get_records(sql)]
        return json.dumps(data)

    @gzipped
    @expose('/data/')
    def data(self):
        """
        Retrieve data from table
        """
        table = request.args.get("table")
        sql = "SELECT * FROM {table} LIMIT 1000;".format(table=table)
        hook = PrestoHook(PRESTO_CONN_ID)
        df = hook.get_pandas_df(sql)
        return df.to_html(
            classes="table table-striped table-bordered table-hover",
            index=False,
            na_rep='',)

    @expose('/ddl/')
    def ddl(self):
        """
        Retrieve table ddl
        """
        table = request.args.get("table")
        sql = "SHOW CREATE TABLE {table};".format(table=table)
        hook = HiveCliHook(HIVE_CLI_CONN_ID)
        return hook.run_cli(sql)


# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "metastore_browser", __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/metastore_browser')


class MetastoreBrowserPlugin(AirflowPlugin):
    """
    Defining the plugin class
    """
    name = "metastore_browser"
    flask_blueprints = [bp]
    appbuilder_views = [{"name": "Hive Metadata Browser",
                         "category": "Plugins",
                         "view": MetastoreBrowserView()}]
