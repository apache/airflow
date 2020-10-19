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
#
"""
This module contains the Great Expectations Hook
"""
import logging
import uuid
import enum

from airflow.hooks.base_hook import BaseHook

from great_expectations.data_context import BaseDataContext

log = logging.getLogger(__name__)


# pylint: disable=too-many-public-methods


class GreatExpectationsHook(BaseHook):
    """
    Interact with the various data sources that GE needs to access to validate data against expectations.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__()

    def create_data_context_config(self):
        """
            Create a GE project configuration that will have the information needed to:
             1.  Connect to the data source containing the data to be validated against expectations.
             2.  Access the expectations file
             3.  Output the validation files
        """
        raise NotImplementedError('Please implement create_custom_datasource() in sub class!')

    def get_batch_kwargs(self):
        """
            Tell GE where to fetch the specific batch of data to be validated.
        """
        raise NotImplementedError('Please implement create_custom_datasource() in sub class!')

    def get_conn(self):
        """
            Returns a GE connection object.
        """
        raise NotImplementedError('Please implement get_conn() in sub class!')

    # Generate a unique name for a temporary table.  For example, if desired_prefix= 'temp_ge_' and
    # desired_length_of_random_portion = 10 then the following table name might be generated: 'temp_ge_304kcj39rM'.
    def get_temp_table_name(self, desired_prefix, desired_length_of_random_portion):
        random_string = str(uuid.uuid4().hex)
        random_portion_of_name = random_string[:desired_length_of_random_portion]
        full_name = desired_prefix + random_portion_of_name
        log.info("Generated name for temporary table: %s", full_name)
        return full_name


class GreatExpectationsConnection:
    """
    GE's concept of a connection is a data context, instantiated from a data context configuration,
    that stores all the connection information for:
        1.  The data source where the data lives that needs to be validated against expectations
        2.  The location of the expectations file
        3.  The location to output the validations and data docs files
    """

    def __init__(self, data_context_config):
        self.data_context_config = data_context_config

    def create_data_context(self):
        # Create a data context from a data context config.
        data_context = BaseDataContext(project_config=self.data_context_config)
        return data_context


class GreatExpectationsValidations(enum.Enum):
    SQL = "SQL"
    TABLE = "TABLE"
