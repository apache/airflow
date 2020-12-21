:mod:`airflow.providers.salesforce.hooks.tableau`
=================================================

.. py:module:: airflow.providers.salesforce.hooks.tableau


Module Contents
---------------

.. py:class:: TableauJobFinishCode

   Bases: :class:`enum.Enum`

   The finish code indicates the status of the job.

   .. seealso:: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#query_job

   .. attribute:: PENDING
      

      

   .. attribute:: SUCCESS
      :annotation: = 0

      

   .. attribute:: ERROR
      :annotation: = 1

      

   .. attribute:: CANCELED
      :annotation: = 2

      


.. py:class:: TableauHook(site_id: Optional[str] = None, tableau_conn_id: str = 'tableau_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Connects to the Tableau Server Instance and allows to communicate with it.

   .. seealso:: https://tableau.github.io/server-client-python/docs/

   :param site_id: The id of the site where the workbook belongs to.
       It will connect to the default site if you don't provide an id.
   :type site_id: Optional[str]
   :param tableau_conn_id: The Tableau Connection id containing the credentials
       to authenticate to the Tableau Server.
   :type tableau_conn_id: str

   
   .. method:: __enter__(self)



   
   .. method:: __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any)



   
   .. method:: get_conn(self)

      Signs in to the Tableau Server and automatically signs out if used as ContextManager.

      :return: an authorized Tableau Server Context Manager object.
      :rtype: tableauserverclient.server.Auth.contextmgr



   
   .. method:: _auth_via_password(self)



   
   .. method:: _auth_via_token(self)



   
   .. method:: get_all(self, resource_name: str)

      Get all items of the given resource.

      .. seealso:: https://tableau.github.io/server-client-python/docs/page-through-results

      :param resource_name: The name of the resource to paginate.
          For example: jobs or workbooks
      :type resource_name: str
      :return: all items by returning a Pager.
      :rtype: tableauserverclient.Pager




