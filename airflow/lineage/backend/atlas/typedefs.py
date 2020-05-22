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
from airflow.configuration import conf
from airflow.lineage.datasets import StandardAirflowOperator, StandardColumn, StandardFile, StandardTable

# relationship name
TABLE_COLUMN_RELATIONSHIP_TYPE = "standard_table_column"
# additional operator attributes which will show as entity property
additional_operator_attributes = conf.get("atlas", "additional_operator_attributes").split(",")


def to_string_attribute(name):
    """
    Generate optional string attribute definition

    :param name: Name of attribute
    :type str
    :return: Attribute definition for rest response
    :type dict
    """
    return {
        "name": name,
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    }


operator_attributes_defs = [
    {
        "name": "dag_id",
        "isOptional": False,
        "isUnique": False,
        "isIndexable": True,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "task_id",
        "isOptional": False,
        "isUnique": False,
        "isIndexable": True,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "command",
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "conn_id",
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "last_execution_date",
        "isOptional": False,
        "isUnique": False,
        "isIndexable": True,
        "typeName": "date",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "start_date",
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "date",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "end_date",
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "date",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    },
    {
        "name": "template_fields",
        "isOptional": True,
        "isUnique": False,
        "isIndexable": False,
        "typeName": "string",
        "valuesMaxCount": 1,
        "cardinality": "SINGLE",
        "valuesMinCount": 0
    }
]

"""
Generate additional operator attributes, these attributes taken from constants package
"""
for attr in additional_operator_attributes:
    operator_attributes_defs.append(to_string_attribute(attr))

operator_typedef = {
    "enumDefs": [],
    "structDefs": [],
    "classificationDefs": [],
    "entityDefs": [
        {
            "superTypes": [
                "Process"
            ],
            "name": StandardAirflowOperator.type_name,
            "description": "Airflow standard Operator",
            "createdBy": "airflow",
            "updatedBy": "airflow",
            "attributeDefs": operator_attributes_defs
        }
    ]
}

standard_table_type = {
    "superTypes": ["DataSet"],
    "name": StandardTable.type_name,
    "description": "Standard table",
    "attributeDefs": [
        {
            "name": "schema_name",
            "typeName": "string"
        },
        {
            "name": "table_name",
            "typeName": "string"
        }
    ],
    "options": {
        "schemaElementsAttribute": "columns"
    },
    "relationshipAttributeDefs": [
        {
            "name": "columns",
            "typeName": "array<{}>".format(StandardColumn.type_name),
            "isOptional": True,
            "cardinality": "SET",
            "valuesMinCount": -1,
            "valuesMaxCount": -1,
            "isUnique": False,
            "isIndexable": False,
            "includeInNotification": False,
            "relationshipTypeName": TABLE_COLUMN_RELATIONSHIP_TYPE,
            "isLegacyAttribute": True
        }
    ]
}

standard_column_type = {
    "superTypes": ["DataSet"],
    "name": StandardColumn.type_name,
    "description": "Standard table column",
    "attributeDefs": [
        {
            "name": "column",
            "typeName": "string"
        },
        {
            "name": "type",
            "typeName": "string"
        },
        {
            "name": "notnull",
            "typeName": "boolean"
        }
    ],
    "options": {
        "schemaAttributes": "[\"column\","
                            " \"type\","
                            " \"notnull\"]"
    },
    "relationshipAttributeDefs": [
        {
            "name": "table",
            "typeName": StandardTable.type_name,
            "isOptional": False,
            "cardinality": "SINGLE",
            "valuesMinCount": -1,
            "valuesMaxCount": -1,
            "isUnique": False,
            "isIndexable": False,
            "includeInNotification": False,
            "relationshipTypeName": TABLE_COLUMN_RELATIONSHIP_TYPE,
            "isLegacyAttribute": True
        },
    ]

}

standard_table_column_relationship_type = {
    "name": TABLE_COLUMN_RELATIONSHIP_TYPE,
    "description": "Standard table to standard column relationship",
    "serviceType": "abstract_source",
    "relationshipCategory": "COMPOSITION",
    "relationshipLabel": "__{}.columns".format(StandardTable.type_name),
    "endDef1": {
        "type": StandardTable.type_name,
        "name": "columns",
        "isContainer": True,
        "cardinality": "SET",
        "isLegacyAttribute": True
    },
    "endDef2": {
        "type": StandardColumn.type_name,
        "name": "table",
        "isContainer": False,
        "cardinality": "SINGLE",
        "isLegacyAttribute": True
    }
}

standard_file_type = {
    "superTypes": ["DataSet"],
    "name": StandardFile.type_name,
    "description": "Standard file",
    "attributeDefs": [
        {
            "name": "path",
            "typeName": "string"
        },
        {
            "name": "cluster_name",
            "typeName": "string"
        }
    ]
}
