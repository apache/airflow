:mod:`airflow.providers.jira.sensors.jira`
==========================================

.. py:module:: airflow.providers.jira.sensors.jira


Module Contents
---------------

.. py:class:: JiraSensor(*, method_name: str, jira_conn_id: str = 'jira_default', method_params: Optional[dict] = None, result_processor: Optional[Callable] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Monitors a jira ticket for any change.

   :param jira_conn_id: reference to a pre-defined Jira Connection
   :type jira_conn_id: str
   :param method_name: method name from jira-python-sdk to be execute
   :type method_name: str
   :param method_params: parameters for the method method_name
   :type method_params: dict
   :param result_processor: function that return boolean and act as a sensor response
   :type result_processor: function

   
   .. method:: poke(self, context: Dict)




.. py:class:: JiraTicketSensor(*, jira_conn_id: str = 'jira_default', ticket_id: Optional[str] = None, field: Optional[str] = None, expected_value: Optional[str] = None, field_checker_func: Optional[Callable] = None, **kwargs)

   Bases: :class:`airflow.providers.jira.sensors.jira.JiraSensor`

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

   .. attribute:: template_fields
      :annotation: = ['ticket_id']

      

   
   .. method:: poke(self, context: Dict)



   
   .. method:: issue_field_checker(self, issue: Issue)

      Check issue using different conditions to prepare to evaluate sensor.




