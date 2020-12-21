:mod:`airflow.providers.zendesk.hooks.zendesk`
==============================================

.. py:module:: airflow.providers.zendesk.hooks.zendesk


Module Contents
---------------

.. py:class:: ZendeskHook(zendesk_conn_id: str)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   A hook to talk to Zendesk

   
   .. method:: get_conn(self)



   
   .. method:: __handle_rate_limit_exception(self, rate_limit_exception: ZendeskError)

      Sleep for the time specified in the exception. If not specified, wait
      for 60 seconds.



   
   .. method:: call(self, path: str, query: Optional[dict] = None, get_all_pages: bool = True, side_loading: bool = False)

      Call Zendesk API and return results

      :param path: The Zendesk API to call
      :param query: Query parameters
      :param get_all_pages: Accumulate results over all pages before
             returning. Due to strict rate limiting, this can often timeout.
             Waits for recommended period between tries after a timeout.
      :param side_loading: Retrieve related records as part of a single
             request. In order to enable side-loading, add an 'include'
             query parameter containing a comma-separated list of resources
             to load. For more information on side-loading see
             https://developer.zendesk.com/rest_api/docs/core/side_loading




