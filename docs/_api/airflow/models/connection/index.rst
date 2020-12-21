:mod:`airflow.models.connection`
================================

.. py:module:: airflow.models.connection


Module Contents
---------------

.. data:: CONN_TYPE_TO_HOOK
   

   

.. function:: parse_netloc_to_hostname(*args, **kwargs)
   This method is deprecated.


.. function:: _parse_netloc_to_hostname(uri_parts)
   Parse a URI string to get correct Hostname.


.. py:class:: Connection(conn_id: Optional[str] = None, conn_type: Optional[str] = None, host: Optional[str] = None, login: Optional[str] = None, password: Optional[str] = None, schema: Optional[str] = None, port: Optional[int] = None, extra: Optional[str] = None, uri: Optional[str] = None)

   Bases: :class:`airflow.models.base.Base`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Placeholder to store information about different database instances
   connection information. The idea here is that scripts use references to
   database instances (conn_id) instead of hard coding hostname, logins and
   passwords when using operators or hooks.

   .. seealso::
       For more information on how to use this class, see: :doc:`/howto/connection/index`

   :param conn_id: The connection ID.
   :type conn_id: str
   :param conn_type: The connection type.
   :type conn_type: str
   :param host: The host.
   :type host: str
   :param login: The login.
   :type login: str
   :param password: The password.
   :type password: str
   :param schema: The schema.
   :type schema: str
   :param port: The port number.
   :type port: int
   :param extra: Extra metadata. Non-standard data such as private/SSH keys can be saved here. JSON
       encoded object.
   :type extra: str
   :param uri: URI address describing connection parameters.
   :type uri: str

   .. attribute:: __tablename__
      :annotation: = connection

      

   .. attribute:: id
      

      

   .. attribute:: conn_id
      

      

   .. attribute:: conn_type
      

      

   .. attribute:: host
      

      

   .. attribute:: schema
      

      

   .. attribute:: login
      

      

   .. attribute:: _password
      

      

   .. attribute:: port
      

      

   .. attribute:: is_encrypted
      

      

   .. attribute:: is_extra_encrypted
      

      

   .. attribute:: _extra
      

      

   .. attribute:: password
      

      Password. The value is decrypted/encrypted when reading/setting the value.


   .. attribute:: extra
      

      Extra data. The value is decrypted/encrypted when reading/setting the value.


   .. attribute:: extra_dejson
      

      Returns the extra property by deserializing json.


   
   .. method:: parse_from_uri(self, **uri)

      This method is deprecated. Please use uri parameter in constructor.



   
   .. method:: _parse_from_uri(self, uri: str)



   
   .. method:: get_uri(self)

      Return connection in URI format



   
   .. method:: get_password(self)

      Return encrypted password.



   
   .. method:: set_password(self, value: Optional[str])

      Encrypt password and set in object attribute.



   
   .. method:: get_extra(self)

      Return encrypted extra-data.



   
   .. method:: set_extra(self, value: str)

      Encrypt extra-data and save in object attribute to object.



   
   .. method:: rotate_fernet_key(self)

      Encrypts data with a new key. See: :ref:`security/fernet`



   
   .. method:: get_hook(self)

      Return hook based on conn_type.



   
   .. method:: __repr__(self)



   
   .. method:: log_info(self)

      This method is deprecated. You can read each field individually or use the
      default representation (`__repr__`).



   
   .. method:: debug_info(self)

      This method is deprecated. You can read each field individually or use the
      default representation (`__repr__`).



   
   .. classmethod:: get_connections_from_secrets(cls, conn_id: str)

      Get all connections as an iterable.

      :param conn_id: connection id
      :return: array of connections




