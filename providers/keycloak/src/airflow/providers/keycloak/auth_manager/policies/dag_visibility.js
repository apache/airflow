/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Keycloak JavaScript policy for batch DAG authorization decisions.
 *
 * Airflow sends all candidate DAG ids in the UMA ticket context attribute
 * ``dag_ids`` (comma separated). This policy inspects each id and adds
 * allowed DAG ids back to the permission as individual scopes so that the
 * Airflow auth manager can map the results efficiently.
 *
 * Customize ``isDagAllowedForUser`` to match your organisation's access model.
 */
var context = $evaluation.getContext();
var attributes = context.getAttributes();

(function () {
    var dagIdsAttr = attributes.getValue("dag_ids");
    if (!dagIdsAttr || dagIdsAttr.isEmpty()) {
        $evaluation.grant();
        return;
    }

    var dagIds = dagIdsAttr.get(0).split(/\s*,\s*/);

    var allowedAttr = attributes.getValue("allowed_dags");
    var allowAll = false;
    var allowedIds = [];
    if (!allowedAttr || allowedAttr.isEmpty()) {
        allowAll = true;
    } else {
        var allowedRaw = allowedAttr.get(0);
        if (allowedRaw === "*") {
            allowAll = true;
        } else if (allowedRaw) {
            var entries = allowedRaw.split(/\s*,\s*/);
            for (var idx = 0; idx < entries.length; idx++) {
                if (entries[idx]) {
                    allowedIds.push(entries[idx]);
                }
            }
        }
    }

    var permission = $evaluation.getPermission();
    var granted = false;

    for (var i = 0; i < dagIds.length; i++) {
        var dagId = dagIds[i];
        if (!dagId) {
            continue;
        }

        if (allowAll) {
            permission.addScope(dagId);
            granted = true;
            continue;
        }

        for (var j = 0; j < allowedIds.length; j++) {
            if (allowedIds[j] === dagId) {
                permission.addScope(dagId);
                granted = true;
                break;
            }
        }
    }

    if (granted) {
        $evaluation.grant();
    } else {
        $evaluation.deny();
    }
})();
