:mod:`airflow.models.crypto`
============================

.. py:module:: airflow.models.crypto


Module Contents
---------------

.. data:: log
   

   

.. py:class:: FernetProtocol

   Bases: :class:`airflow.typing_compat.Protocol`

   This class is only used for TypeChecking (for IDEs, mypy, pylint, etc)

   
   .. method:: decrypt(self, b)

      Decrypt with Fernet



   
   .. method:: encrypt(self, b)

      Encrypt with Fernet




.. py:class:: NullFernet

   A "Null" encryptor class that doesn't encrypt or decrypt but that presents
   a similar interface to Fernet.

   The purpose of this is to make the rest of the code not have to know the
   difference, and to only display the message once, not 20 times when
   `airflow db init` is ran.

   .. attribute:: is_encrypted
      :annotation: = False

      

   
   .. method:: decrypt(self, b)

      Decrypt with Fernet.



   
   .. method:: encrypt(self, b)

      Encrypt with Fernet.




.. data:: _fernet
   :annotation: :Optional[FernetProtocol]

   

.. function:: get_fernet()
   Deferred load of Fernet key.

   This function could fail either because Cryptography is not installed
   or because the Fernet key is invalid.

   :return: Fernet object
   :raises: airflow.exceptions.AirflowException if there's a problem trying to load Fernet


