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

import React, { forwardRef } from "react";
import { Flex } from "@chakra-ui/react";

import { getMetaValue, appendSearchParams } from "src/utils";
import LinkButton from "src/components/LinkButton";
import type { Task } from "src/types";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";

const dagId = getMetaValue("dag_id");
const isK8sExecutor = getMetaValue("k8s_or_k8scelery_executor") === "True";
const taskInstancesUrl = getMetaValue("task_instances_list_url");
const renderedK8sUrl = getMetaValue("rendered_k8s_url");
const taskUrl = getMetaValue("task_url");
const gridUrl = getMetaValue("grid_url");

interface Props {
  taskId: Task["id"];
  executionDate: string;
  operator?: string;
  isMapped?: boolean;
  mapIndex?: number;
}

const Nav = forwardRef<HTMLDivElement, Props>(
  ({ taskId, executionDate, operator, isMapped = false, mapIndex }, ref) => {
    if (!taskId) return null;
    const params = new URLSearchParamsWrapper({
      task_id: taskId,
      execution_date: executionDate,
      map_index: mapIndex ?? -1,
    });
    const detailsLink = `${taskUrl}&${params}`;
    const k8sLink = `${renderedK8sUrl}&${params}`;
    const listParams = new URLSearchParamsWrapper({
      _flt_3_dag_id: dagId,
      _flt_3_task_id: taskId,
      _oc_TaskInstanceModelView: "dag_run.execution_date",
    });
    const subDagParams = new URLSearchParamsWrapper({
      execution_date: executionDate,
    }).toString();

    if (mapIndex !== undefined && mapIndex >= 0)
      listParams.append("_flt_0_map_index", mapIndex.toString());

    const allInstancesLink = `${taskInstancesUrl}?${listParams.toString()}`;

    const subDagLink = appendSearchParams(
      gridUrl.replace(dagId, `${dagId}.${taskId}`),
      subDagParams
    );

    // TODO: base subdag zooming as its own attribute instead of via operator name
    const isSubDag = operator === "SubDagOperator";

    return (
      <Flex flexWrap="wrap" ref={ref} mb={2}>
        {(!isMapped || mapIndex !== undefined) && (
          <>
            <LinkButton href={detailsLink}>More Details</LinkButton>
            {isK8sExecutor && (
              <LinkButton href={k8sLink}>K8s Pod Spec</LinkButton>
            )}
            {isSubDag && (
              <LinkButton href={subDagLink}>Zoom into SubDag</LinkButton>
            )}
          </>
        )}
        <LinkButton href={allInstancesLink} title="View all">
          List All Instances
        </LinkButton>
      </Flex>
    );
  }
);

export default Nav;
