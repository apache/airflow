'''
Imports the hooks dynamically while keeping the package API clean,
abstracting the underlying modules
'''
from airflow.utils import import_module_attrs as _import_module_attrs
from airflow.hooks.base_hook import BaseHook as _BaseHook

_hooks = {
    'hive_hooks': [
        'HiveCliHook',
        'HiveMetastoreHook',
        'HiveServer2Hook',
    ],
    'hdfs_hook': ['HDFSHook'],
    'mysql_hook': ['MySqlHook'],
    'postgres_hook': ['PostgresHook'],
    'presto_hook': ['PrestoHook'],
    'samba_hook': ['SambaHook'],
    'sqlite_hook': ['SqliteHook'],
    'S3_hook': ['S3Hook'],
    'http_hook': ['HttpHook'],
    'druid_hook': ['DruidHook'],
    'jdbc_hook': ['JdbcHook'],
    'dbapi_hook': ['DbApiHook'],
    'mssql_hook': ['MsSqlHook'],
}

_import_module_attrs(globals(), _hooks)


def integrate_plugins():
    """Integrate plugins to the context"""
    from airflow.plugins_manager import hooks as _hooks
    for _h in _hooks:
        globals()[_h.__name__] = _h
