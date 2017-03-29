# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from jira.resources import Resource

from airflow.contrib.operators.jira_operator import JIRAError
from airflow.contrib.operators.jira_operator import JiraOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class JiraSensor(BaseSensorOperator):
    """
    Monitors a jira ticket for any change.

    :param jira_conn_id: reference to a pre-defined Jira Connection
    :type jira_conn_id: str
    :param method_name: method name from jira-python-sdk to be execute
    :type method_name: str
    :param method_params: parameters for the method method_name
    :type method_params: dict
    :param result_processor: function that return boolean and act as a sensor response
    :type result_processor: function
    """

    @apply_defaults
    def __init__(self,
                 jira_conn_id='jira_default',
                 method_name=None,
                 method_params=None,
                 result_processor=None,
                 *args,
                 **kwargs):
        super(JiraSensor, self).__init__(*args, **kwargs)
        self.jira_conn_id = jira_conn_id
        self.result_processor = None
        if result_processor is not None:
            self.result_processor = result_processor
        self.method_name = method_name
        self.method_params = method_params
        self.jira_operator = JiraOperator(task_id=self.task_id,
                                          jira_conn_id=self.jira_conn_id,
                                          jira_method=self.method_name,
                                          jira_method_args=self.method_params,
                                          result_processor=self.result_processor)

    def poke(self, context):
        return self.jira_operator.execute(context=context)


class JiraTicketSensor(JiraSensor):
    """
    Monitors a jira ticket for given change in terms of function.

    :param jira_conn_id: reference to a pre-defined Jira Connection
    :type jira_conn_id: str
    :param ticket_id: id of the ticket to be monitored
    :type ticket_id: str
    :param field: field of the ticket to be monitored
    :type field: str
    :param expected_value: expected value of the field
    :type expected_value: str
    :param result_processor: function that return boolean and act as a sensor response
    :type result_processor: function
    """

    template_fields = ("ticket_id",)

    @apply_defaults
    def __init__(self,
                 jira_conn_id='jira_default',
                 ticket_id=None,
                 field=None,
                 expected_value=None,
                 field_checker_func=None,
                 *args, **kwargs):

        self.jira_conn_id = jira_conn_id
        self.ticket_id = ticket_id
        self.field = field
        self.expected_value = expected_value
        if field_checker_func is None:
            field_checker_func = self.issue_field_checker

        super(JiraTicketSensor, self).__init__(jira_conn_id=jira_conn_id,
                                               result_processor=field_checker_func,
                                               *args, **kwargs)

    def poke(self, context):
        logging.info('Jira Sensor checking for change in ticket : {0}'
                     .format(self.ticket_id))

        self.jira_operator.method_name = "issue"
        self.jira_operator.jira_method_args = {
            'id': self.ticket_id,
            'fields': self.field
        }
        return JiraSensor.poke(self, context=context)

    def issue_field_checker(self, context, issue):
        result = None
        try:
            if issue is not None \
                    and self.field is not None \
                    and self.expected_value is not None:

                field_value = getattr(issue.fields, self.field)
                if field_value is not None:
                    if isinstance(field_value, list):
                        result = self.expected_value in field_value
                    elif isinstance(field_value, str):
                        result = self.expected_value.lower() == field_value.lower()
                    elif isinstance(field_value, Resource) \
                            and getattr(field_value, 'name'):
                        result = self.expected_value.lower() == field_value.name.lower()
                    else:
                        logging.warning("not implemented checker for issue field {0} "
                                        "which is neither string nor list nor "
                                        "jira Resource".format(self.field))

        except JIRAError as jira_error:
            logging.error("jira error while checking with expected value: {0}"
                          .format(jira_error))
        except Exception as e:
            logging.error("error while checking with expected value {0}, error: {1}"
                          .format(self.expected_value, e))
        if result is True:
            logging.info("issue field {0} has expected value {1}, returning success"
                         .format(self.field, self.expected_value))
        else:
            logging.info("issue field {0} dont have expected value {1} yet."
                         .format(self.field, self.expected_value))
        return result
