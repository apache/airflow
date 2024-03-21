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
# fmt: off
"""
Operators.

:sphinx-autoapi-skip:
"""

from __future__ import annotations

from airflow.utils.deprecation_tools import add_deprecated_classes

__deprecated_classes = {
    "bash_operator": {
        "BashOperator": "airflow.operators.bash.BashOperator",
    },
    "branch_operator": {
        "BaseBranchOperator": "airflow.operators.branch.BaseBranchOperator",
    },
    "check_operator": {
        "SQLCheckOperator": "airflow.providers.common.sql.operators.sql.SQLCheckOperator",
        "SQLIntervalCheckOperator": "airflow.providers.common.sql.operators.sql.SQLIntervalCheckOperator",
        "SQLThresholdCheckOperator": "airflow.providers.common.sql.operators.sql.SQLThresholdCheckOperator",
        "SQLValueCheckOperator": "airflow.providers.common.sql.operators.sql.SQLValueCheckOperator",
        "CheckOperator": "airflow.providers.common.sql.operators.sql.SQLCheckOperator",
        "IntervalCheckOperator": "airflow.providers.common.sql.operators.sql.SQLIntervalCheckOperator",
        "ThresholdCheckOperator": "airflow.providers.common.sql.operators.sql.SQLThresholdCheckOperator",
        "ValueCheckOperator": "airflow.providers.common.sql.operators.sql.SQLValueCheckOperator",
    },
    "dagrun_operator": {
        "TriggerDagRunLink": "airflow.operators.trigger_dagrun.TriggerDagRunLink",
        "TriggerDagRunOperator": "airflow.operators.trigger_dagrun.TriggerDagRunOperator",
    },
    "docker_operator": {
        "DockerOperator": "airflow.providers.docker.operators.docker.DockerOperator",
    },
    "druid_check_operator": {
        "DruidCheckOperator": "airflow.providers.apache.druid.operators.druid_check.DruidCheckOperator",
    },
    "dummy": {
        "EmptyOperator": "airflow.operators.empty.EmptyOperator",
        "DummyOperator": "airflow.operators.empty.EmptyOperator",
    },
    "dummy_operator": {
        "EmptyOperator": "airflow.operators.empty.EmptyOperator",
        "DummyOperator": "airflow.operators.empty.EmptyOperator",
    },
    "email_operator": {
        "EmailOperator": "airflow.operators.email.EmailOperator",
    },
    "gcs_to_s3": {
        "GCSToS3Operator": "airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSToS3Operator",
    },
    "google_api_to_s3_transfer": {
        "GoogleApiToS3Operator": (
            "airflow.providers.amazon.aws.transfers.google_api_to_s3.GoogleApiToS3Operator"
        ),
        "GoogleApiToS3Transfer": (
            "airflow.providers.amazon.aws.transfers.google_api_to_s3.GoogleApiToS3Operator"
        ),
    },
    "hive_operator": {
        "HiveOperator": "airflow.providers.apache.hive.operators.hive.HiveOperator",
    },
    "hive_stats_operator": {
        "HiveStatsCollectionOperator": (
            "airflow.providers.apache.hive.operators.hive_stats.HiveStatsCollectionOperator"
        ),
    },
    "hive_to_druid": {
        "HiveToDruidOperator": "airflow.providers.apache.druid.transfers.hive_to_druid.HiveToDruidOperator",
        "HiveToDruidTransfer": "airflow.providers.apache.druid.transfers.hive_to_druid.HiveToDruidOperator",
    },
    "hive_to_mysql": {
        "HiveToMySqlOperator": "airflow.providers.apache.hive.transfers.hive_to_mysql.HiveToMySqlOperator",
        "HiveToMySqlTransfer": "airflow.providers.apache.hive.transfers.hive_to_mysql.HiveToMySqlOperator",
    },
    "hive_to_samba_operator": {
        "HiveToSambaOperator": "airflow.providers.apache.hive.transfers.hive_to_samba.HiveToSambaOperator",
    },
    "http_operator": {
        "SimpleHttpOperator": "airflow.providers.http.operators.http.SimpleHttpOperator",
    },
    "jdbc_operator": {
        "JdbcOperator": "airflow.providers.jdbc.operators.jdbc.JdbcOperator",
    },
    "latest_only_operator": {
        "LatestOnlyOperator": "airflow.operators.latest_only.LatestOnlyOperator",
    },
    "mssql_operator": {
        "MsSqlOperator": "airflow.providers.microsoft.mssql.operators.mssql.MsSqlOperator",
    },
    "mssql_to_hive": {
        "MsSqlToHiveOperator": "airflow.providers.apache.hive.transfers.mssql_to_hive.MsSqlToHiveOperator",
        "MsSqlToHiveTransfer": "airflow.providers.apache.hive.transfers.mssql_to_hive.MsSqlToHiveOperator",
    },
    "mysql_operator": {
        "MySqlOperator": "airflow.providers.mysql.operators.mysql.MySqlOperator",
    },
    "mysql_to_hive": {
        "MySqlToHiveOperator": "airflow.providers.apache.hive.transfers.mysql_to_hive.MySqlToHiveOperator",
        "MySqlToHiveTransfer": "airflow.providers.apache.hive.transfers.mysql_to_hive.MySqlToHiveOperator",
    },
    "oracle_operator": {
        "OracleOperator": "airflow.providers.oracle.operators.oracle.OracleOperator",
    },
    "papermill_operator": {
        "PapermillOperator": "airflow.providers.papermill.operators.papermill.PapermillOperator",
    },
    "pig_operator": {
        "PigOperator": "airflow.providers.apache.pig.operators.pig.PigOperator",
    },
    "postgres_operator": {
        "Mapping": "airflow.providers.postgres.operators.postgres.Mapping",
        "PostgresOperator": "airflow.providers.postgres.operators.postgres.PostgresOperator",
    },
    "presto_check_operator": {
        "SQLCheckOperator": "airflow.providers.common.sql.operators.sql.SQLCheckOperator",
        "SQLIntervalCheckOperator": "airflow.providers.common.sql.operators.sql.SQLIntervalCheckOperator",
        "SQLValueCheckOperator": "airflow.providers.common.sql.operators.sql.SQLValueCheckOperator",
        "PrestoCheckOperator": "airflow.providers.common.sql.operators.sql.SQLCheckOperator",
        "PrestoIntervalCheckOperator": "airflow.providers.common.sql.operators.sql.SQLIntervalCheckOperator",
        "PrestoValueCheckOperator": "airflow.providers.common.sql.operators.sql.SQLValueCheckOperator",
    },
    "presto_to_mysql": {
        "PrestoToMySqlOperator": "airflow.providers.mysql.transfers.presto_to_mysql.PrestoToMySqlOperator",
        "PrestoToMySqlTransfer": "airflow.providers.mysql.transfers.presto_to_mysql.PrestoToMySqlOperator",
    },
    "python_operator": {
        "BranchPythonOperator": "airflow.operators.python.BranchPythonOperator",
        "PythonOperator": "airflow.operators.python.PythonOperator",
        "PythonVirtualenvOperator": "airflow.operators.python.PythonVirtualenvOperator",
        "ShortCircuitOperator": "airflow.operators.python.ShortCircuitOperator",
    },
    "redshift_to_s3_operator": {
        "RedshiftToS3Operator": "airflow.providers.amazon.aws.transfers.redshift_to_s3.RedshiftToS3Operator",
        "RedshiftToS3Transfer": "airflow.providers.amazon.aws.transfers.redshift_to_s3.RedshiftToS3Operator",
    },
    "s3_file_transform_operator": {
        "S3FileTransformOperator": (
            "airflow.providers.amazon.aws.operators.s3_file_transform.S3FileTransformOperator"
        ),
    },
    "s3_to_hive_operator": {
        "S3ToHiveOperator": "airflow.providers.apache.hive.transfers.s3_to_hive.S3ToHiveOperator",
        "S3ToHiveTransfer": "airflow.providers.apache.hive.transfers.s3_to_hive.S3ToHiveOperator",
    },
    "s3_to_redshift_operator": {
        "S3ToRedshiftOperator": "airflow.providers.amazon.aws.transfers.s3_to_redshift.S3ToRedshiftOperator",
        "S3ToRedshiftTransfer": "airflow.providers.amazon.aws.transfers.s3_to_redshift.S3ToRedshiftOperator",
    },
    "slack_operator": {
        "SlackAPIOperator": "airflow.providers.slack.operators.slack.SlackAPIOperator",
        "SlackAPIPostOperator": "airflow.providers.slack.operators.slack.SlackAPIPostOperator",
    },
    "sql": {
        "BaseSQLOperator": "airflow.providers.common.sql.operators.sql.BaseSQLOperator",
        "BranchSQLOperator": "airflow.providers.common.sql.operators.sql.BranchSQLOperator",
        "SQLCheckOperator": "airflow.providers.common.sql.operators.sql.SQLCheckOperator",
        "SQLColumnCheckOperator": "airflow.providers.common.sql.operators.sql.SQLColumnCheckOperator",
        "SQLIntervalCheckOperator": "airflow.providers.common.sql.operators.sql.SQLIntervalCheckOperator",
        "SQLTableCheckOperator": "airflow.providers.common.sql.operators.sql.SQLTableCheckOperator",
        "SQLThresholdCheckOperator": "airflow.providers.common.sql.operators.sql.SQLThresholdCheckOperator",
        "SQLValueCheckOperator": "airflow.providers.common.sql.operators.sql.SQLValueCheckOperator",
        "_convert_to_float_if_possible": (
            "airflow.providers.common.sql.operators.sql._convert_to_float_if_possible"
        ),
        "parse_boolean": "airflow.providers.common.sql.operators.sql.parse_boolean",
    },
    "sql_branch_operator": {
        "BranchSQLOperator": "airflow.providers.common.sql.operators.sql.BranchSQLOperator",
        "BranchSqlOperator": "airflow.providers.common.sql.operators.sql.BranchSQLOperator",
    },
    "sqlite_operator": {
        "SqliteOperator": "airflow.providers.sqlite.operators.sqlite.SqliteOperator",
    },
    "subdag_operator": {
        "SkippedStatePropagationOptions": "airflow.operators.subdag.SkippedStatePropagationOptions",
        "SubDagOperator": "airflow.operators.subdag.SubDagOperator",
    },
}

add_deprecated_classes(__deprecated_classes, __name__)
