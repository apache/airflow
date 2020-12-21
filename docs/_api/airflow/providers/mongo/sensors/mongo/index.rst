:mod:`airflow.providers.mongo.sensors.mongo`
============================================

.. py:module:: airflow.providers.mongo.sensors.mongo


Module Contents
---------------

.. py:class:: MongoSensor(*, collection: str, query: dict, mongo_conn_id: str = 'mongo_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Checks for the existence of a document which
   matches the given query in MongoDB. Example:

   >>> mongo_sensor = MongoSensor(collection="coll",
   ...                            query={"key": "value"},
   ...                            mongo_conn_id="mongo_default",
   ...                            task_id="mongo_sensor")

   :param collection: Target MongoDB collection.
   :type collection: str
   :param query: The query to find the target document.
   :type query: dict
   :param mongo_conn_id: The connection ID to use
       when connecting to MongoDB.
   :type mongo_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['collection', 'query']

      

   
   .. method:: poke(self, context: dict)




