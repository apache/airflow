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
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Iterable, Sequence, cast

from bson import json_util

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook

if TYPE_CHECKING:
    from pymongo.command_cursor import CommandCursor
    from pymongo.cursor import Cursor

    from airflow.utils.context import Context


class MongoToS3Operator(BaseOperator):
    """
    Move data from MongoDB to S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MongoToS3Operator`

    :param mongo_conn_id: reference to a specific mongo connection
    :param aws_conn_id: reference to a specific S3 connection
    :param mongo_collection: reference to a specific collection in your mongo db
    :param mongo_query: query to execute. A list including a dict of the query
    :param mongo_projection: optional parameter to filter the returned fields by
        the query. It can be a list of fields names to include or a dictionary
        for excluding fields (e.g ``projection={"_id": 0}`` )
    :param s3_bucket: reference to a specific S3 bucket to store the data
    :param s3_key: in which S3 key the file will be stored
    :param mongo_db: reference to a specific mongo database
    :param replace: whether or not to replace the file in S3 if it previously existed
    :param allow_disk_use: enables writing to temporary files in the case you are handling large dataset.
        This only takes effect when `mongo_query` is a list - running an aggregate pipeline
    :param compression: type of compression to use for output file in S3. Currently only gzip is supported.
    """

    template_fields: Sequence[str] = ("s3_bucket", "s3_key", "mongo_query", "mongo_collection")
    ui_color = "#589636"
    template_fields_renderers = {"mongo_query": "json"}

    def __init__(
        self,
        *,
        mongo_conn_id: str = "mongo_default",
        aws_conn_id: str | None = "aws_default",
        mongo_collection: str,
        mongo_query: list | dict,
        s3_bucket: str,
        s3_key: str,
        mongo_db: str | None = None,
        mongo_projection: list | dict | None = None,
        replace: bool = False,
        allow_disk_use: bool = False,
        compression: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.aws_conn_id = aws_conn_id
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection

        # Grab query and determine if we need to run an aggregate pipeline
        self.mongo_query = mongo_query
        self.is_pipeline = isinstance(self.mongo_query, list)
        self.mongo_projection = mongo_projection

        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.replace = replace
        self.allow_disk_use = allow_disk_use
        self.compression = compression

    def execute(self, context: Context):
        """Is written to depend on transform method."""
        s3_conn = S3Hook(self.aws_conn_id)

        # Grab collection and execute query according to whether or not it is a pipeline
        if self.is_pipeline:
            results: CommandCursor[Any] | Cursor = MongoHook(self.mongo_conn_id).aggregate(
                mongo_collection=self.mongo_collection,
                aggregate_query=cast(list, self.mongo_query),
                mongo_db=self.mongo_db,
                allowDiskUse=self.allow_disk_use,
            )

        else:
            results = MongoHook(self.mongo_conn_id).find(
                mongo_collection=self.mongo_collection,
                query=cast(dict, self.mongo_query),
                projection=self.mongo_projection,
                mongo_db=self.mongo_db,
                find_one=False,
            )

        # Performs transform then stringifies the docs results into json format
        docs_str = self._stringify(self.transform(results))

        s3_conn.load_string(
            string_data=docs_str,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=self.replace,
            compression=self.compression,
        )

    @staticmethod
    def _stringify(iterable: Iterable, joinable: str = "\n") -> str:
        """
        Stringify an iterable of dicts.

        This dumps each dict with JSON, and joins them with ``joinable``.
        """
        return joinable.join(json.dumps(doc, default=json_util.default) for doc in iterable)

    @staticmethod
    def transform(docs: Any) -> Any:
        """
        Transform the data for transfer.

        This method is meant to be extended by child classes to perform
        transformations unique to those operators needs. Processes pyMongo
        cursor and returns an iterable with each element being a JSON
        serializable dictionary

        The default implementation assumes no processing is needed, i.e. input
        is a pyMongo cursor of documents and just needs to be passed through.

        Override this method for custom transformations.
        """
        return docs
