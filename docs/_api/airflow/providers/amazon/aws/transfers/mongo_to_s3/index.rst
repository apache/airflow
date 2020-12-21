:mod:`airflow.providers.amazon.aws.transfers.mongo_to_s3`
=========================================================

.. py:module:: airflow.providers.amazon.aws.transfers.mongo_to_s3


Module Contents
---------------

.. py:class:: MongoToS3Operator(*, mongo_conn_id: str, s3_conn_id: str, mongo_collection: str, mongo_query: Union[list, dict], s3_bucket: str, s3_key: str, mongo_db: Optional[str] = None, replace: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Mongo -> S3
   A more specific baseOperator meant to move data
   from mongo via pymongo to s3 via boto

   things to note
           .execute() is written to depend on .transform()
           .transform() is meant to be extended by child classes
           to perform transformations unique to those operators needs

   .. attribute:: template_fields
      :annotation: = ['s3_key', 'mongo_query']

      

   
   .. method:: execute(self, context)

      Executed by task_instance at runtime



   
   .. staticmethod:: _stringify(iterable: Iterable, joinable: str = '\n')

      Takes an iterable (pymongo Cursor or Array) containing dictionaries and
      returns a stringified version using python join



   
   .. staticmethod:: transform(docs: Any)

      Processes pyMongo cursor and returns an iterable with each element being
              a JSON serializable dictionary

      Base transform() assumes no processing is needed
      ie. docs is a pyMongo cursor of documents and cursor just
      needs to be passed through

      Override this method for custom transformations




