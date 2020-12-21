:mod:`airflow.security.kerberos`
================================

.. py:module:: airflow.security.kerberos

.. autoapi-nested-parse::

   Kerberos security provider



Module Contents
---------------

.. data:: NEED_KRB181_WORKAROUND
   :annotation: :Optional[bool]

   

.. data:: log
   

   

.. function:: renew_from_kt(principal: str, keytab: str, exit_on_fail: bool = True)
   Renew kerberos token from keytab

   :param principal: principal
   :param keytab: keytab file
   :return: None


.. function:: perform_krb181_workaround(principal: str)
   Workaround for Kerberos 1.8.1.

   :param principal: principal name
   :return: None


.. function:: detect_conf_var() -> bool
   Return true if the ticket cache contains "conf" information as is found
   in ticket caches of Kerberos 1.8.1 or later. This is incompatible with the
   Sun Java Krb5LoginModule in Java6, so we need to take an action to work
   around it.


.. function:: run(principal: str, keytab: str)
   Run the kerbros renewer.

   :param principal: principal name
   :param keytab: keytab file
   :return: None


