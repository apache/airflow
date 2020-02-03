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

import logging

from airflow import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class BaseDataQualityOperator(BaseOperator):
    """
    BaseDataQualityOperator is an abstract base operator class to
    perform data quality checks

    :param sql: sql (or path to sql) code to be executed
    :type sql: str
    :param conn_type: database type
    :type conn_type: str
    :param conn_id: connection id of database
    :type conn_id: str
    :param push_conn_type: (optional) external database type
    :type push_conn_type: str
    :param push_conn_id: (optional) connection id of external database
    :type push_conn_id: str
    :param check_description: (optional) description of data quality sql statement
    :type check_description: str
    """

    template_fields = ['sql']
    template_ext = ['.sql']

    @apply_defaults
    def __init__(self,
                 sql,
                 conn_id,
                 push_conn_id=None,
                 check_description=None,
                 *args,
                 **kwargs
                 ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.push_conn_id = push_conn_id
        self.sql = sql
        self.check_description = check_description

    def execute(self, context):
        """Method where data quality check is performed """
        raise NotImplementedError

    def push(self, info_dict):
        """
        Optional: Send data check info and metadata to an external database.
        Default functionality will log metadata.
        """
        info = "\n".join([f"""{key}: {item}""" for key, item in info_dict.items()])
        log.info("Log from %s:\n%s", self.dag_id, info)

    def send_failure_notification(self, info_dict):
        """
        send_failure_notification will throw an AirflowException with logging
        information and dq check results from the failed task that was just run.
        """

        body = (f'Data Quality Check: "{info_dict.get("task_id")}" failed.\n'
                f'DAG: {self.dag_id}\nTask_id: {info_dict.get("task_id")}\n'
                f'Check description: {info_dict.get("description")}\n'
                f'Execution date: {info_dict.get("execution_date")}\n'
                f'SQL: {self.sql}\n'
                f'Result: {round(info_dict.get("result"), 2)} is not within thresholds '
                f'{info_dict.get("min_threshold")} and {info_dict.get("max_threshold")}'
                )
        raise AirflowException(body)

    def get_sql_value(self, conn_id, sql):
        """
        get_sql_value executes a sql query given proper connection parameters.
        The result of the sql query should be one and only one numeric value.
        """
        hook = BaseHook.get_connection(conn_id).get_hook()
        result = hook.get_records(sql)
        if len(result) > 1:
            raise ValueError("Result from sql query contains more than 1 entry")
        elif len(result) < 1:
            raise ValueError("No result returned from sql query")
        elif len(result[0]) != 1:
            raise ValueError("Result from sql query does not contain exactly 1 column")
        else:
            return result[0][0]


class DataQualityThresholdCheckOperator(BaseDataQualityOperator):
    """
    DataQualityThresholdCheckOperator builds off BaseOperator and
    executes a data quality check against high & low threshold values.

    :param min_threshold: lower-bound value
    :type min_threshold: numeric
    :param max_threshold: upper-bound value
    :type max_threshold: numeric
    """

    @apply_defaults
    def __init__(self,
                 min_threshold,
                 max_threshold,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.min_threshold = min_threshold
        self.max_threshold = max_threshold

    def execute(self, context):
        result = self.get_sql_value(self.conn_id, self.sql)
        info_dict = {
            "result": result,
            "description": self.check_description,
            "task_id": self.task_id,
            "execution_date": context.get("execution_date"),
            "min_threshold": self.min_threshold,
            "max_threshold": self.max_threshold,
            "within_threshold": self.min_threshold <= result <= self.max_threshold
        }

        self.push(info_dict)
        if not info_dict["within_threshold"]:
            context["ti"].xcom_push(key=f"""result data from task {self.task_id}""", value=info_dict)
            self.send_failure_notification(info_dict)
        return info_dict


class DataQualityThresholdSQLCheckOperator(BaseDataQualityOperator):
    """
    DataQualityThresholdSQLCheckOperator inherits from DataQualityThresholdCheckOperator.
    This operator will first calculate the min and max threshold values with given sql
    statements from a defined source, evaluate the data quality check, and then compare
    that result to the min and max thresholds calculated.

    :param min_threshold_sql: lower bound sql statement (or path to sql statement)
    :type min_threshold_sql: str
    :param max_threshold_sql: upper bound sql statement (or path to sql statement)
    :type max_threshold_sql: str
    :param threshold_conn_type: connection type of threshold sql statement table
    :type threshold_conn_type: str
    :param threshold_conn_id: connection id of threshold sql statement table
    :type threshold_conn_id: str
    """

    template_fields = ['sql', 'min_threshold_sql', 'max_threshold_sql']
    template_ext = ['.sql']

    @apply_defaults
    def __init__(self,
                 min_threshold_sql,
                 max_threshold_sql,
                 threshold_conn_id,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.min_threshold_sql = min_threshold_sql
        self.max_threshold_sql = max_threshold_sql
        self.threshold_conn_id = threshold_conn_id

    def execute(self, context):
        min_threshold = self.get_sql_value(self.threshold_conn_id, self.min_threshold_sql)
        max_threshold = self.get_sql_value(self.threshold_conn_id, self.max_threshold_sql)

        result = self.get_sql_value(self.conn_id, self.sql)
        info_dict = {
            "result": result,
            "description": self.check_description,
            "task_id": self.task_id,
            "execution_date": context.get("execution_date"),
            "min_threshold": min_threshold,
            "max_threshold": max_threshold,
            "within_threshold": min_threshold <= result <= max_threshold
        }

        self.push(info_dict)
        if not info_dict["within_threshold"]:
            context["ti"].xcom_push(key=f"""result data from task {self.task_id}""", value=info_dict)
            self.send_failure_notification(info_dict)
        return info_dict
