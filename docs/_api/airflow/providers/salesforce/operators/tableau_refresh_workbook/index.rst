:mod:`airflow.providers.salesforce.operators.tableau_refresh_workbook`
======================================================================

.. py:module:: airflow.providers.salesforce.operators.tableau_refresh_workbook


Module Contents
---------------

.. py:class:: TableauRefreshWorkbookOperator(*, workbook_name: str, site_id: Optional[str] = None, blocking: bool = True, tableau_conn_id: str = 'tableau_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Refreshes a Tableau Workbook/Extract

   .. seealso:: https://tableau.github.io/server-client-python/docs/api-ref#workbooks

   :param workbook_name: The name of the workbook to refresh.
   :type workbook_name: str
   :param site_id: The id of the site where the workbook belongs to.
   :type site_id: Optional[str]
   :param blocking: By default the extract refresh will be blocking means it will wait until it has finished.
   :type blocking: bool
   :param tableau_conn_id: The Tableau Connection id containing the credentials
       to authenticate to the Tableau Server.
   :type tableau_conn_id: str

   
   .. method:: execute(self, context: dict)

      Executes the Tableau Extract Refresh and pushes the job id to xcom.

      :param context: The task context during execution.
      :type context: dict
      :return: the id of the job that executes the extract refresh
      :rtype: str



   
   .. method:: _get_workbook_by_name(self, tableau_hook: TableauHook)



   
   .. method:: _refresh_workbook(self, tableau_hook: TableauHook, workbook_id: str)




