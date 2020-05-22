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
This module responsible for sending lineage data.

This module require next properties in airflow.cfg:

    [lineage]
    backend = airflow.lineage.backend.atlas.AtlasBackend

    [atlas]
        username = username
        password = password
        host = host
        port = port
        create_if_not_exist = True
        fail_if_lineage_error = True
        timeout = 600
"""

import json
import logging

from atlasclient.client import Atlas
from atlasclient.exceptions import HttpError

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.lineage.backend import LineageBackend
from airflow.lineage.backend.atlas import typedefs
from airflow.lineage.datasets import StandardAirflowOperator
from airflow.utils.timezone import convert_to_utc

SERIALIZED_DATE_FORMAT_STR = "%Y-%m-%dT%H:%M:%S.%fZ"
atlas_conn_id = "atlas_default"

_username = conf.get("atlas", "username")
_password = conf.get("atlas", "password")
_port = conf.get("atlas", "port")
_host = conf.get("atlas", "host")

# additional operator attributes which will show as entity property
_additional_operator_attributes = conf.get("atlas", "additional_operator_attributes").split(",")
# If true, missing input and output atlas entities will create/update, false
_create_if_not_exists = conf.getboolean("atlas", "create_if_not_exist")
# If true, operator will be failed in error in sending lineage data, false
_fail_if_lineage_error = conf.getboolean("atlas", "fail_if_lineage_error")
# timeout for atlas rest api client
_timeout = conf.getint("atlas", "timeout")


class AtlasBackend(LineageBackend):
    """
    A class what should be used in airflow.cfg as backend lineage class.
    This class responsible for sending data to atlas.
    """

    def send_lineage(self, operator=None, inlets=None, outlets=None, context=None):
        """
        :param operator: Airflow operator what was executed
        :type BaseOperator
        :param inlets: dict contains datasets what used as inputs for lineage flow,
        should contain key "dataset", and corresponding value should contain list of atlas entities object.
        All avaliable atlas entities class can be in models.datasets
        :type dict
        :param outlets: dict contains datasets what used as outputs for lineage flow. Same as inlets.
         See comments above.
        :type dict
        :param context: Airflow task instance context
        :return: Nothing
        """
        try:
            AtlasBackend.__send_lineage(operator, inlets, outlets, context)
        except HttpError as err:

            logging.error("Error in sending lineage data")
            logging.error(err)
            if _fail_if_lineage_error:
                raise AirflowException(err)

    @staticmethod
    def __send_lineage(operator, inlets, outlets, context):
        logging.info("Start sending lineage data")
        logging.info("Create input and output entities options is " +
                     "enabled" if _create_if_not_exists else "disabled")
        host = _host
        port = _port
        username = _username
        password = _password
        logging.info("Host: %s, port: %s", host, port)

        client = Atlas(host=host, port=port, username=username, password=password, timeout=_timeout)
        types_data = {
            "entityDefs": [
                typedefs.standard_table_type,
                typedefs.standard_column_type,
                typedefs.standard_file_type],
            "relationshipDefs": [typedefs.standard_table_column_relationship_type]
        }

        try:
            logging.info("Start creating/updating airflow lineage datasets types")
            client.typedefs.create(data=types_data)
        except HttpError as err:
            logging.info("Update types. Details: %s", err.details)
            client.typedefs.update(data=types_data)

        try:
            logging.info("Start creating/updating airflow operator lineage type")
            client.typedefs.create(data=typedefs.operator_typedef)
        except HttpError as err:
            logging.info("Update types. Details: %s", err.details)
            client.typedefs.update(data=typedefs.operator_typedef)

        logging.info("Types was created/updated")
        _execution_date = convert_to_utc(context['ti'].execution_date)
        _start_date = convert_to_utc(context['ti'].start_date)
        _end_date = convert_to_utc(context['ti'].end_date)
        inlet_list = []
        if inlets:
            for entity in inlets:
                if entity is None:
                    continue
                entity.set_context(context)
                if _create_if_not_exists:
                    AtlasBackend._create_entity(client=client, entity=entity)
                inlet_list.append({"typeName": entity.type_name,
                                   "uniqueAttributes": {
                                       "qualifiedName": entity.qualified_name}})

        outlet_list = []
        if outlets:
            for entity in outlets:
                if not entity:
                    continue

                entity.set_context(context)
                if _create_if_not_exists:
                    AtlasBackend._create_entity(client=client, entity=entity)
                outlet_list.append({"typeName": entity.type_name,
                                    "uniqueAttributes": {
                                        "qualifiedName": entity.qualified_name}})

        template_fields = AtlasBackend.template_fields_to_string(operator)
        operator_name = operator.__class__.__name__
        name = "{} {} ({})".format(operator.dag_id, operator.task_id, operator_name)
        qualified_name = "{}.{}.{}".format(operator.dag_id,
                                           operator.task_id,
                                           operator_name)

        data = {
            "dag_id": operator.dag_id,
            "task_id": operator.task_id,
            "task_type": operator_name,
            "last_execution_date": _execution_date.strftime(SERIALIZED_DATE_FORMAT_STR),
            "name": name,
            "inputs": inlet_list,
            "outputs": outlet_list,
            "template_fields": template_fields
        }

        for attr in _additional_operator_attributes:
            data[attr] = getattr(operator, attr, None)

        if _start_date:
            data["start_date"] = _start_date.strftime(SERIALIZED_DATE_FORMAT_STR)
        if _end_date:
            data["end_date"] = _end_date.strftime(SERIALIZED_DATE_FORMAT_STR)

        process = StandardAirflowOperator(qualified_name=qualified_name,
                                          data=data,
                                          additional_operator_attributes=_additional_operator_attributes)
        AtlasBackend._create_entity(client=client, entity=process)
        logging.info("Lineage data sent successfully")

    @staticmethod
    def template_fields_to_string(operator):
        """
        :param operator: Airflow operator what was executed
        :return: Json string representing all template fields in operator.
        """
        result = dict()
        for field in operator.__class__.template_fields:
            result[field] = str(getattr(operator, field, None))
        return json.dumps(result, indent=2)

    @staticmethod
    def _create_entity(client, entity):
        """
        Create Atlas entity

        :param client: atlas client
        :param data: dict with entity data
        :return: Nothing
        """

        data = {"entity": entity.as_dict()}
        result = client.entity_post.create(data=data)
        guid = AtlasBackend._get_response_guid(result)
        logging.info("Entity %s=%s created/updated", entity.type_name, guid)

    @staticmethod
    def _get_response_guid(response):
        """
        Get guid of created entity.
        If the entity already exists, just return guid of existing entity.
        """
        try:
            guid = response["mutatedEntities"]["CREATE"][0]["guid"]
        except KeyError:
            guid = list(response["guidAssignments"].values())[0]
        return guid
