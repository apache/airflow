# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json

from bson import json_util

from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults


class MongoToS3Operator(BaseOperator):
    """
    Mongo -> S3
        A more specific baseOperator meant to move data
        from mongo via pymongo to s3 via boto

        things to note
                .execute() is written to depend on .transform()
                .transform() is meant to be extended by child classes
                to perform transformations unique to those operators needs
    """

    template_fields = ['s3_key', 'mongo_query']
    # pylint: disable=too-many-instance-attributes

    @apply_defaults
    def __init__(self,
                 mongo_conn_id,
                 s3_conn_id,
                 mongo_collection,
                 mongo_query,
                 s3_bucket,
                 s3_key,
                 mongo_db=None,
                 replace=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Conn Ids
        self.mongo_conn_id = mongo_conn_id
        self.s3_conn_id = s3_conn_id
        # Mongo Query Settings
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        # Grab query and determine if we need to run an aggregate pipeline
        self.mongo_query = mongo_query
        self.is_pipeline = True if isinstance(
            self.mongo_query, list) else False

        # S3 Settings
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.replace = replace

    def execute(self, context):
        """
        Executed by task_instance at runtime
        """
        s3_conn = S3Hook(self.s3_conn_id)

        # Grab collection and execute query according to whether or not it is a pipeline
        if self.is_pipeline:
            results = MongoHook(self.mongo_conn_id).aggregate(
                mongo_collection=self.mongo_collection,
                aggregate_query=self.mongo_query,
                mongo_db=self.mongo_db
            )

        else:
            results = MongoHook(self.mongo_conn_id).find(
                mongo_collection=self.mongo_collection,
                query=self.mongo_query,
                mongo_db=self.mongo_db
            )

        # Performs transform then stringifies the docs results into json format
        docs_str = self._stringify(self.transform(results))

        # Load Into S3
        s3_conn.load_string(
            string_data=docs_str,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=self.replace
        )

        return True

    @staticmethod
    def _stringify(iterable, joinable='\n'):
        """
        Takes an iterable (pymongo Cursor or Array) containing dictionaries and
        returns a stringified version using python join
        """
        return joinable.join(
            [json.dumps(doc, default=json_util.default) for doc in iterable]
        )

    @staticmethod
    def transform(docs):
        """
        Processes pyMongo cursor and returns an iterable with each element being
                a JSON serializable dictionary

        Base transform() assumes no processing is needed
        ie. docs is a pyMongo cursor of documents and cursor just
        needs to be passed through

        Override this method for custom transformations
        """
        return docs
