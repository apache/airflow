:mod:`airflow.security.utils`
=============================

.. py:module:: airflow.security.utils

.. autoapi-nested-parse::

   Various security-related utils.



Module Contents
---------------

.. function:: get_components(principal)
   get_components(principal) -> (short name, instance (FQDN), realm)

   ``principal`` is the kerberos principal to parse.


.. function:: replace_hostname_pattern(components, host=None)
   Replaces hostname with the right pattern including lowercase of the name.


.. function:: get_fqdn(hostname_or_ip=None)
   Retrieves FQDN - hostname for the IP or hostname.


.. function:: principal_from_username(username, realm)
   Retrieves principal from the user name and realm.


