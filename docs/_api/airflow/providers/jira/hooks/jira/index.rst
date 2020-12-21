:mod:`airflow.providers.jira.hooks.jira`
========================================

.. py:module:: airflow.providers.jira.hooks.jira

.. autoapi-nested-parse::

   Hook for JIRA



Module Contents
---------------

.. py:class:: JiraHook(jira_conn_id: str = 'jira_default', proxies: Optional[Any] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Jira interaction hook, a Wrapper around JIRA Python SDK.

   :param jira_conn_id: reference to a pre-defined Jira Connection
   :type jira_conn_id: str

   
   .. method:: get_conn(self)




