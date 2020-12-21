:mod:`airflow.providers.mongo.hooks.mongo`
==========================================

.. py:module:: airflow.providers.mongo.hooks.mongo

.. autoapi-nested-parse::

   Hook for Mongo DB



Module Contents
---------------

.. py:class:: MongoHook(conn_id: str = 'mongo_default', *args, **kwargs)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   PyMongo Wrapper to Interact With Mongo Database
   Mongo Connection Documentation
   https://docs.mongodb.com/manual/reference/connection-string/index.html
   You can specify connection string options in extra field of your connection
   https://docs.mongodb.com/manual/reference/connection-string/index.html#connection-string-options

   If you want use DNS seedlist, set `srv` to True.

   ex.
       {"srv": true, "replicaSet": "test", "ssl": true, "connectTimeoutMS": 30000}

   .. attribute:: conn_type
      :annotation: = mongo

      

   
   .. method:: __enter__(self)



   
   .. method:: __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType])



   
   .. method:: get_conn(self)

      Fetches PyMongo Client



   
   .. method:: close_conn(self)

      Closes connection



   
   .. method:: get_collection(self, mongo_collection: str, mongo_db: Optional[str] = None)

      Fetches a mongo collection object for querying.

      Uses connection schema as DB unless specified.



   
   .. method:: aggregate(self, mongo_collection: str, aggregate_query: list, mongo_db: Optional[str] = None, **kwargs)

      Runs an aggregation pipeline and returns the results
      https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.aggregate
      https://api.mongodb.com/python/current/examples/aggregation.html



   
   .. method:: find(self, mongo_collection: str, query: dict, find_one: bool = False, mongo_db: Optional[str] = None, **kwargs)

      Runs a mongo find query and returns the results
      https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find



   
   .. method:: insert_one(self, mongo_collection: str, doc: dict, mongo_db: Optional[str] = None, **kwargs)

      Inserts a single document into a mongo collection
      https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.insert_one



   
   .. method:: insert_many(self, mongo_collection: str, docs: dict, mongo_db: Optional[str] = None, **kwargs)

      Inserts many docs into a mongo collection.
      https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.insert_many



   
   .. method:: update_one(self, mongo_collection: str, filter_doc: dict, update_doc: dict, mongo_db: Optional[str] = None, **kwargs)

      Updates a single document in a mongo collection.
      https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.update_one

      :param mongo_collection: The name of the collection to update.
      :type mongo_collection: str
      :param filter_doc: A query that matches the documents to update.
      :type filter_doc: dict
      :param update_doc: The modifications to apply.
      :type update_doc: dict
      :param mongo_db: The name of the database to use.
          Can be omitted; then the database from the connection string is used.
      :type mongo_db: str



   
   .. method:: update_many(self, mongo_collection: str, filter_doc: dict, update_doc: dict, mongo_db: Optional[str] = None, **kwargs)

      Updates one or more documents in a mongo collection.
      https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.update_many

      :param mongo_collection: The name of the collection to update.
      :type mongo_collection: str
      :param filter_doc: A query that matches the documents to update.
      :type filter_doc: dict
      :param update_doc: The modifications to apply.
      :type update_doc: dict
      :param mongo_db: The name of the database to use.
          Can be omitted; then the database from the connection string is used.
      :type mongo_db: str



   
   .. method:: replace_one(self, mongo_collection: str, doc: dict, filter_doc: Optional[dict] = None, mongo_db: Optional[str] = None, **kwargs)

      Replaces a single document in a mongo collection.
      https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.replace_one

      .. note::
          If no ``filter_doc`` is given, it is assumed that the replacement
          document contain the ``_id`` field which is then used as filters.

      :param mongo_collection: The name of the collection to update.
      :type mongo_collection: str
      :param doc: The new document.
      :type doc: dict
      :param filter_doc: A query that matches the documents to replace.
          Can be omitted; then the _id field from doc will be used.
      :type filter_doc: dict
      :param mongo_db: The name of the database to use.
          Can be omitted; then the database from the connection string is used.
      :type mongo_db: str



   
   .. method:: replace_many(self, mongo_collection: str, docs: List[dict], filter_docs: Optional[List[dict]] = None, mongo_db: Optional[str] = None, upsert: bool = False, collation: Optional[pymongo.collation.Collation] = None, **kwargs)

      Replaces many documents in a mongo collection.

      Uses bulk_write with multiple ReplaceOne operations
      https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.bulk_write

      .. note::
          If no ``filter_docs``are given, it is assumed that all
          replacement documents contain the ``_id`` field which are then
          used as filters.

      :param mongo_collection: The name of the collection to update.
      :type mongo_collection: str
      :param docs: The new documents.
      :type docs: list[dict]
      :param filter_docs: A list of queries that match the documents to replace.
          Can be omitted; then the _id fields from docs will be used.
      :type filter_docs: list[dict]
      :param mongo_db: The name of the database to use.
          Can be omitted; then the database from the connection string is used.
      :type mongo_db: str
      :param upsert: If ``True``, perform an insert if no documents
          match the filters for the replace operation.
      :type upsert: bool
      :param collation: An instance of
          :class:`~pymongo.collation.Collation`. This option is only
          supported on MongoDB 3.4 and above.
      :type collation: pymongo.collation.Collation



   
   .. method:: delete_one(self, mongo_collection: str, filter_doc: dict, mongo_db: Optional[str] = None, **kwargs)

      Deletes a single document in a mongo collection.
      https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.delete_one

      :param mongo_collection: The name of the collection to delete from.
      :type mongo_collection: str
      :param filter_doc: A query that matches the document to delete.
      :type filter_doc: dict
      :param mongo_db: The name of the database to use.
          Can be omitted; then the database from the connection string is used.
      :type mongo_db: str



   
   .. method:: delete_many(self, mongo_collection: str, filter_doc: dict, mongo_db: Optional[str] = None, **kwargs)

      Deletes one or more documents in a mongo collection.
      https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.delete_many

      :param mongo_collection: The name of the collection to delete from.
      :type mongo_collection: str
      :param filter_doc: A query that matches the documents to delete.
      :type filter_doc: dict
      :param mongo_db: The name of the database to use.
          Can be omitted; then the database from the connection string is used.
      :type mongo_db: str




