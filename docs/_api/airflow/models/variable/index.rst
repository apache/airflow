:mod:`airflow.models.variable`
==============================

.. py:module:: airflow.models.variable


Module Contents
---------------

.. py:class:: Variable(key=None, val=None)

   Bases: :class:`airflow.models.base.Base`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Variables are a generic way to store and retrieve arbitrary content or settings
   as a simple key value store within Airflow.

   .. attribute:: __tablename__
      :annotation: = variable

      

   .. attribute:: __NO_DEFAULT_SENTINEL
      

      

   .. attribute:: id
      

      

   .. attribute:: key
      

      

   .. attribute:: _val
      

      

   .. attribute:: is_encrypted
      

      

   .. attribute:: val
      

      Get Airflow Variable from Metadata DB and decode it using the Fernet Key


   
   .. method:: __repr__(self)



   
   .. method:: get_val(self)

      Get Airflow Variable from Metadata DB and decode it using the Fernet Key



   
   .. method:: set_val(self, value)

      Encode the specified value with Fernet Key and store it in Variables Table.



   
   .. classmethod:: setdefault(cls, key, default, deserialize_json=False)

      Like a Python builtin dict object, setdefault returns the current value
      for a key, and if it isn't there, stores the default value and returns it.

      :param key: Dict key for this Variable
      :type key: str
      :param default: Default value to set and return if the variable
          isn't already in the DB
      :type default: Mixed
      :param deserialize_json: Store this as a JSON encoded value in the DB
          and un-encode it when retrieving a value
      :return: Mixed



   
   .. classmethod:: get(cls, key: str, default_var: Any = __NO_DEFAULT_SENTINEL, deserialize_json: bool = False)

      Sets a value for an Airflow Key

      :param key: Variable Key
      :param default_var: Default value of the Variable if the Variable doesn't exists
      :param deserialize_json: Deserialize the value to a Python dict



   
   .. classmethod:: set(cls, key: str, value: Any, serialize_json: bool = False, session: Session = None)

      Sets a value for an Airflow Variable with a given Key

      :param key: Variable Key
      :param value: Value to set for the Variable
      :param serialize_json: Serialize the value to a JSON string
      :param session: SQL Alchemy Sessions



   
   .. classmethod:: delete(cls, key: str, session: Session = None)

      Delete an Airflow Variable for a given key

      :param key: Variable Key
      :param session: SQL Alchemy Sessions



   
   .. method:: rotate_fernet_key(self)

      Rotate Fernet Key



   
   .. staticmethod:: get_variable_from_secrets(key: str)

      Get Airflow Variable by iterating over all Secret Backends.

      :param key: Variable Key
      :return: Variable Value




