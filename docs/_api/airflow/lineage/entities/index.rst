:mod:`airflow.lineage.entities`
===============================

.. py:module:: airflow.lineage.entities

.. autoapi-nested-parse::

   Defines the base entities that can be used for providing lineage
   information.



Module Contents
---------------

.. py:class:: File

   File entity. Refers to a file

   .. attribute:: url
      :annotation: :str

      

   .. attribute:: type_hint
      :annotation: :Optional[str]

      


.. py:class:: User

   User entity. Identifies a user

   .. attribute:: email
      :annotation: :str

      

   .. attribute:: first_name
      :annotation: :Optional[str]

      

   .. attribute:: last_name
      :annotation: :Optional[str]

      


.. py:class:: Tag

   Tag or classification entity.

   .. attribute:: tag_name
      :annotation: :str

      


.. py:class:: Column

   Column of a Table

   .. attribute:: name
      :annotation: :str

      

   .. attribute:: description
      :annotation: :Optional[str]

      

   .. attribute:: data_type
      :annotation: :str

      

   .. attribute:: tags
      :annotation: :List[Tag] = []

      


.. function:: default_if_none(arg: Optional[bool]) -> bool

.. py:class:: Table

   Table entity

   .. attribute:: database
      :annotation: :str

      

   .. attribute:: cluster
      :annotation: :str

      

   .. attribute:: name
      :annotation: :str

      

   .. attribute:: tags
      :annotation: :List[Tag] = []

      

   .. attribute:: description
      :annotation: :Optional[str]

      

   .. attribute:: columns
      :annotation: :List[Column] = []

      

   .. attribute:: owners
      :annotation: :List[User] = []

      

   .. attribute:: extra
      :annotation: :Dict[str, Any]

      

   .. attribute:: type_hint
      :annotation: :Optional[str]

      


