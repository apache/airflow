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
    'ftp_hook': ['FTPHook'],
}

_import_module_attrs(globals(), _hooks)


def integrate_plugins():
    """Integrate plugins to the context"""
    from airflow.plugins_manager import get_plugins
    for _plugin in get_plugins(_BaseHook):
        globals()[_plugin.__name__] = _plugin
