:mod:`airflow.providers.jira.operators.jira`
============================================

.. py:module:: airflow.providers.jira.operators.jira


Module Contents
---------------

.. py:class:: JiraOperator(*, jira_method: str, jira_conn_id: str = 'jira_default', jira_method_args: Optional[dict] = None, result_processor: Optional[Callable] = None, get_jira_resource_method: Optional[Callable] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   JiraOperator to interact and perform action on Jira issue tracking system.
   This operator is designed to use Jira Python SDK: http://jira.readthedocs.io

   :param jira_conn_id: reference to a pre-defined Jira Connection
   :type jira_conn_id: str
   :param jira_method: method name from Jira Python SDK to be called
   :type jira_method: str
   :param jira_method_args: required method parameters for the jira_method. (templated)
   :type jira_method_args: dict
   :param result_processor: function to further process the response from Jira
   :type result_processor: function
   :param get_jira_resource_method: function or operator to get jira resource
                                   on which the provided jira_method will be executed
   :type get_jira_resource_method: function

   .. attribute:: template_fields
      :annotation: = ['jira_method_args']

      

   
   .. method:: execute(self, context: Dict)




