dbapi is extracted from https://github.com/ydb-platform/ydb-sqlalchemy/releases/tag/0.0.1b22 (Apache License 2.0) to avoid dependency on sqlalchemy package ver > 2.
_vendor could be removed in favor of ydb-sqlalchemy package after switching Airflow core to sqlalchemy > 2 (related issue https://github.com/apache/airflow/issues/28723).
Another option is to wait for separate package for ydb-dbapi: https://github.com/ydb-platform/ydb-sqlalchemy/issues/46 and switch to it afterwards.
