from datetime import datetime
import json

from flask import Blueprint, request
from flask.ext.admin import BaseView, expose
import pandas as pd

from airflow.hooks import HiveMetastoreHook, MySqlHook, PrestoHook, HiveCliHook
from airflow.plugins_manager import AirflowPlugin
from airflow.www import utils as wwwutils

METASTORE_CONN_ID = 'metastore_default'
METASTORE_MYSQL_CONN_ID = 'metastore_mysql'
PRESTO_CONN_ID = 'presto_default'
HIVE_CLI_CONN_ID = 'hive_default'
DEFAULT_DB = 'default'
DB_WHITELIST = None
DB_BLACKLIST = ['tmp']
TABLE_SELECTOR_LIMIT = 2000

# Keeping pandas from truncating long strings
pd.set_option('display.max_colwidth', -1)


# Creating a flask admin BaseView
class MetastoreBrowserView(BaseView, wwwutils.DataProfilingMixin):

    @expose('/')
    def index(self):
        sql = """
        SELECT
            a.name as db, db_location_uri as location,
            count(1) as object_count, a.desc as description
        FROM DBS a
        JOIN TBLS b ON a.DB_ID = b.DB_ID
        GROUP BY a.name, db_location_uri, a.desc
        """.format(**locals())
        h = MySqlHook(METASTORE_MYSQL_CONN_ID)
        df = h.get_pandas_df(sql)
        df.db = (
            '<a href="/admin/metastorebrowserview/db/?db=' +
            df.db + '">' + df.db + '</a>')
        table = df.to_html(
            classes="table table-striped table-bordered table-hover",
            index=False,
            escape=False,
            na_rep='',)
        return self.render(
            "metastore_browser/dbs.html", table=table)

    @expose('/table/')
    def table(self):
        table_name = request.args.get("table")
        m = HiveMetastoreHook(METASTORE_CONN_ID)
        table = m.get_table(table_name)
        return self.render(
            "metastore_browser/table.html",
            table=table, table_name=table_name, datetime=datetime, int=int)

    @expose('/db/')
    def db(self):
        db = request.args.get("db")
        m = HiveMetastoreHook(METASTORE_CONN_ID)
        tables = sorted(m.get_tables(db=db), key=lambda x: x.tableName)
        return self.render(
            "metastore_browser/db.html", tables=tables, db=db)

    @wwwutils.gzipped
    @expose('/partitions/')
    def partitions(self):
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
        """.format(**locals())
        h = MySqlHook(METASTORE_MYSQL_CONN_ID)
        df = h.get_pandas_df(sql)
        return df.to_html(
            classes="table table-striped table-bordered table-hover",
            index=False,
            na_rep='',)

    @wwwutils.gzipped
    @expose('/objects/')
    def objects(self):
        where_clause = ''
        if DB_WHITELIST:
            dbs = ",".join(["'" + db + "'" for db in DB_WHITELIST])
            where_clause = "AND b.name IN ({})".format(dbs)
        if DB_BLACKLIST:
            dbs = ",".join(["'" + db + "'" for db in DB_BLACKLIST])
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
        h = MySqlHook(METASTORE_MYSQL_CONN_ID)
        d = [
                {'id': row[0], 'text': row[0]}
            for row in h.get_records(sql)]
        return json.dumps(d)

    @wwwutils.gzipped
    @expose('/data/')
    def data(self):
        table = request.args.get("table")
        sql = "SELECT * FROM {table} LIMIT 1000;".format(table=table)
        h = PrestoHook(PRESTO_CONN_ID)
        df = h.get_pandas_df(sql)
        return df.to_html(
            classes="table table-striped table-bordered table-hover",
            index=False,
            na_rep='',)

    @expose('/ddl/')
    def ddl(self):
        table = request.args.get("table")
        sql = "SHOW CREATE TABLE {table};".format(table=table)
        h = HiveCliHook(HIVE_CLI_CONN_ID)
        return h.run_cli(sql)

v = MetastoreBrowserView(category="Plugins", name="Hive Metadata Browser")

# Creating a flask blueprint to intergrate the templates and static folder
bp = Blueprint(
    "metastore_browser", __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/metastore_browser')


# Defining the plugin class
class MetastoreBrowserPlugin(AirflowPlugin):
    name = "metastore_browser"
    flask_blueprints = [bp]
    admin_views = [v]
