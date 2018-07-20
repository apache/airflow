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
#

operator_typedef = {
    "enumDefs": [],
    "structDefs": [],
    "classificationDefs": [],
    "entityDefs": [
        {
            "superTypes": [
                "Process"
            ],
            "name": "airflow_operator",
            "description": "Airflow Operator",
            "createdBy": "airflow",
            "updatedBy": "airflow",
            "attributeDefs": [
                # "name" will be set to Operator name
                # "qualifiedName" will be set to dag_id_task_id@operator_name
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
                    "name": "execution_date",
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
            ],
        },
    ],
}
