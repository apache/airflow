import MySQLdb
import MySQLdb.cursors

from airflow.hooks.dbapi_hook import DbApiHook


class MySqlHook(DbApiHook):
    '''
    Interact with MySQL.

    You can specify charset in the extra field of your connection
    as ``{"charset": "utf8"}``. Also you can choose cursor as
    ``{"cursor": "SSCursor"}``. Refer to the MySQLdb.cursors for more details.
    '''

    conn_name_attr = 'mysql_conn_id'
    default_conn_name = 'mysql_default'
    supports_autocommit = True

    def get_conn(self):
        """
        Returns a mysql connection object
        """
        conn = self.get_connection(self.mysql_conn_id)
        conn_config = {
            "user": conn.login,
            "passwd": conn.password
        }

        conn_config["host"] = conn.host or 'localhost'
        if not conn.port:
            conn_config["port"] = 3306
        else:
            conn_config["port"] = int(conn.port)
        if conn.schema:
            conn_config["db"] = conn.schema
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

        conn = MySQLdb.connect(**conn_config)
        return conn
