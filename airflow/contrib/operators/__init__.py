# Imports the operators dynamically while keeping the package API clean,
# abstracting the underlying modules
from airflow.utils import import_module_attrs as _import_module_attrs

_operators = {
    'vertica_operator': ['VerticaOperator'],
    'vertica_to_hive': ['VerticaToHiveTransfer'],
}

_import_module_attrs(globals(), _operators)
