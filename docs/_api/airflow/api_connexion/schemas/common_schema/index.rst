:mod:`airflow.api_connexion.schemas.common_schema`
==================================================

.. py:module:: airflow.api_connexion.schemas.common_schema


Module Contents
---------------

.. py:class:: CronExpression

   Bases: :class:`typing.NamedTuple`

   Cron expression schema

   .. attribute:: value
      :annotation: :str

      


.. py:class:: TimeDeltaSchema

   Bases: :class:`marshmallow.Schema`

   Time delta schema

   .. attribute:: objectType
      

      

   .. attribute:: days
      

      

   .. attribute:: seconds
      

      

   .. attribute:: microseconds
      

      

   
   .. method:: make_time_delta(self, data, **kwargs)

      Create time delta based on data




.. py:class:: RelativeDeltaSchema

   Bases: :class:`marshmallow.Schema`

   Relative delta schema

   .. attribute:: objectType
      

      

   .. attribute:: years
      

      

   .. attribute:: months
      

      

   .. attribute:: days
      

      

   .. attribute:: leapdays
      

      

   .. attribute:: hours
      

      

   .. attribute:: minutes
      

      

   .. attribute:: seconds
      

      

   .. attribute:: microseconds
      

      

   .. attribute:: year
      

      

   .. attribute:: month
      

      

   .. attribute:: day
      

      

   .. attribute:: hour
      

      

   .. attribute:: minute
      

      

   .. attribute:: second
      

      

   .. attribute:: microsecond
      

      

   
   .. method:: make_relative_delta(self, data, **kwargs)

      Create relative delta based on data




.. py:class:: CronExpressionSchema

   Bases: :class:`marshmallow.Schema`

   Cron expression schema

   .. attribute:: objectType
      

      

   .. attribute:: value
      

      

   
   .. method:: make_cron_expression(self, data, **kwargs)

      Create cron expression based on data




.. py:class:: ScheduleIntervalSchema

   Bases: :class:`marshmallow_oneofschema.OneOfSchema`

   Schedule interval.

   It supports the following types:

   * TimeDelta
   * RelativeDelta
   * CronExpression

   .. attribute:: type_field
      :annotation: = __type

      

   .. attribute:: type_schemas
      

      

   
   .. method:: _dump(self, obj, update_fields=True, **kwargs)



   
   .. method:: get_obj_type(self, obj)

      Select schema based on object type




.. py:class:: ColorField(**metadata)

   Bases: :class:`marshmallow.fields.String`

   Schema for color property


.. py:class:: WeightRuleField(**metadata)

   Bases: :class:`marshmallow.fields.String`

   Schema for WeightRule


.. py:class:: TimezoneField

   Bases: :class:`marshmallow.fields.String`

   Schema for timezone


.. py:class:: ClassReferenceSchema

   Bases: :class:`marshmallow.Schema`

   Class reference schema.

   .. attribute:: module_path
      

      

   .. attribute:: class_name
      

      

   
   .. method:: _get_module(self, obj)



   
   .. method:: _get_class_name(self, obj)




