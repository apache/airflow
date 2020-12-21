:mod:`airflow.www.extensions.init_security`
===========================================

.. py:module:: airflow.www.extensions.init_security


Module Contents
---------------

.. data:: log
   

   

.. function:: init_xframe_protection(app)
   Add X-Frame-Options header. Use it to avoid click-jacking attacks, by ensuring that their content is not
   embedded into other sites.

   See also: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Frame-Options


.. function:: init_api_experimental_auth(app)
   Loads authentication backend


