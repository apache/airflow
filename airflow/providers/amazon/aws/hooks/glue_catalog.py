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

"""
This module contains AWS Glue Catalog Hook
"""
from airflow.providers.amazon.aws.hooks.aws_hook import AwsHook


class AwsGlueCatalogHook(AwsHook):
    """
    Interact with AWS Glue Catalog

    :param aws_conn_id: ID of the Airflow connection where
        credentials and extra configuration are stored
    :type aws_conn_id: str
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    """

    def __init__(self,
                 aws_conn_id='aws_default',
                 region_name=None,
                 *args,
                 **kwargs):
        self.region_name = region_name
        self.conn = None
        super().__init__(aws_conn_id=aws_conn_id, *args, **kwargs)

    def get_conn(self):
        """
        Returns glue connection object.
        """
        self.conn = self.get_client_type('glue', self.region_name)
        return self.conn

    def get_partitions(self,
                       database_name,
                       table_name,
                       expression='',
                       page_size=None,
                       max_items=None):
        """
        Retrieves the partition values for a table.

        :param database_name: The name of the catalog database where the partitions reside.
        :type database_name: str
        :param table_name: The name of the partitions' table.
        :type table_name: str
        :param expression: An expression filtering the partitions to be returned.
            Please see official AWS documentation for further information.
            https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetPartitions
        :type expression: str
        :param page_size: pagination size
        :type page_size: int
        :param max_items: maximum items to return
        :type max_items: int
        :return: set of partition values where each value is a tuple since
            a partition may be composed of multiple columns. For example:
            ``{('2018-01-01','1'), ('2018-01-01','2')}``
        """
        config = {
            'PageSize': page_size,
            'MaxItems': max_items,
        }

        paginator = self.get_conn().get_paginator('get_partitions')
        response = paginator.paginate(
            DatabaseName=database_name,
            TableName=table_name,
            Expression=expression,
            PaginationConfig=config
        )

        partitions = set()
        for page in response:
            for partition in page['Partitions']:
                partitions.add(tuple(partition['Values']))

        return partitions

    def check_for_partition(self, database_name, table_name, expression):
        """
        Checks whether a partition exists

        :param database_name: Name of hive database (schema) @table belongs to
        :type database_name: str
        :param table_name: Name of hive table @partition belongs to
        :type table_name: str
        :expression: Expression that matches the partitions to check for
            (eg `a = 'b' AND c = 'd'`)
        :type expression: str
        :rtype: bool

        >>> hook = AwsGlueCatalogHook()
        >>> t = 'static_babynames_partitioned'
        >>> hook.check_for_partition('airflow', t, "ds='2015-01-01'")
        True
        """
        partitions = self.get_partitions(database_name, table_name, expression, max_items=1)

        return bool(partitions)

    def get_table(self, database_name, table_name):
        """
        Get the information of the table

        :param database_name: Name of hive database (schema) @table belongs to
        :type database_name: str
        :param table_name: Name of hive table
        :type table_name: str
        :rtype: dict

        >>> hook = AwsGlueCatalogHook()
        >>> r = hook.get_table('db', 'table_foo')
        >>> r['Name'] = 'table_foo'
        """

        result = self.get_conn().get_table(DatabaseName=database_name, Name=table_name)

        return result['Table']

    def get_table_location(self, database_name, table_name):
        """
        Get the physical location of the table

        :param database_name: Name of hive database (schema) @table belongs to
        :type database_name: str
        :param table_name: Name of hive table
        :type table_name: str
        :return: str
        """

        table = self.get_table(database_name, table_name)

        return table['StorageDescriptor']['Location']
