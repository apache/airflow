:mod:`airflow.api_connexion.schemas.config_schema`
==================================================

.. py:module:: airflow.api_connexion.schemas.config_schema


Module Contents
---------------

.. py:class:: ConfigOptionSchema

   Bases: :class:`marshmallow.Schema`

   Config Option Schema

   .. attribute:: key
      

      

   .. attribute:: value
      

      


.. py:class:: ConfigOption

   Bases: :class:`typing.NamedTuple`

   Config option

   .. attribute:: key
      :annotation: :str

      

   .. attribute:: value
      :annotation: :str

      


.. py:class:: ConfigSectionSchema

   Bases: :class:`marshmallow.Schema`

   Config Section Schema

   .. attribute:: name
      

      

   .. attribute:: options
      

      


.. py:class:: ConfigSection

   Bases: :class:`typing.NamedTuple`

   List of config options within a section

   .. attribute:: name
      :annotation: :str

      

   .. attribute:: options
      :annotation: :List[ConfigOption]

      


.. py:class:: ConfigSchema

   Bases: :class:`marshmallow.Schema`

   Config Schema

   .. attribute:: sections
      

      


.. py:class:: Config

   Bases: :class:`typing.NamedTuple`

   List of config sections with their options

   .. attribute:: sections
      :annotation: :List[ConfigSection]

      


.. data:: config_schema
   

   

