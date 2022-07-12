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

import React from 'react';
import {
  Flex,
  Divider,
} from '@chakra-ui/react';

import { getMetaValue, appendSearchParams } from 'app/utils';
import LinkButton from 'grid/components/LinkButton';
import type { Task, DagRun } from 'grid/types';

const dagId = getMetaValue('dag_id') || '';
const isK8sExecutor = getMetaValue('k8s_or_k8scelery_executor') === 'True';
const numRuns = getMetaValue('num_runs');
const baseDate = getMetaValue('base_date');
const taskInstancesUrl = getMetaValue('task_instances_list_url');
const renderedK8sUrl = getMetaValue('rendered_k8s_url');
const renderedTemplatesUrl = getMetaValue('rendered_templates_url');
const xcomUrl = getMetaValue('xcom_url');
const logUrl = getMetaValue('log_url');
const taskUrl = getMetaValue('task_url');
const gridUrl = getMetaValue('grid_url') || '';
const gridUrlNoRoot = getMetaValue('grid_url_no_root');

interface Props {
  runId: DagRun['runId'];
  taskId: Task['id'];
  executionDate: string;
  operator: string;
  isMapped?: boolean;
}

const Nav = ({
  runId, taskId, executionDate, operator, isMapped = false,
}: Props) => {
  if (!taskId) return null;
  const params = new URLSearchParams({
    task_id: taskId,
    execution_date: executionDate,
  }).toString();
  const detailsLink = `${taskUrl}&${params}`;
  const renderedLink = `${renderedTemplatesUrl}&${params}`;
  const logLink = `${logUrl}&${params}`;
  const xcomLink = `${xcomUrl}&${params}`;
  const k8sLink = `${renderedK8sUrl}&${params}`;
  const listParams = new URLSearchParams({
    _flt_3_dag_id: dagId,
    _flt_3_task_id: taskId,
    _oc_TaskInstanceModelView: 'dag_run.execution_date',
  });
  const subDagParams = new URLSearchParams({
    execution_date: executionDate,
  }).toString();

  const filterParams = new URLSearchParams({
    task_id: taskId,
    dag_run_id: runId,
    root: taskId,
  });

  if (baseDate) filterParams.append('base_date', baseDate);
  if (numRuns) filterParams.append('num_runs', numRuns);

  const allInstancesLink = `${taskInstancesUrl}?${listParams.toString()}`;

  const filterUpstreamLink = appendSearchParams(gridUrlNoRoot, filterParams.toString());
  const subDagLink = appendSearchParams(gridUrl.replace(dagId, `${dagId}.${taskId}`), subDagParams);

  // TODO: base subdag zooming as its own attribute instead of via operator name
  const isSubDag = operator === 'SubDagOperator';

  return (
    <>
      <Flex flexWrap="wrap">
        {!isMapped && (
        <>
          <LinkButton href={detailsLink}>Task Instance Details</LinkButton>
          <LinkButton href={renderedLink}>Rendered Template</LinkButton>
          {isK8sExecutor && (
          <LinkButton href={k8sLink}>K8s Pod Spec</LinkButton>
          )}
          {isSubDag && (
          <LinkButton href={subDagLink}>Zoom into SubDag</LinkButton>
          )}
          <LinkButton href={logLink}>Log</LinkButton>
          <LinkButton href={xcomLink}>XCom</LinkButton>
        </>
        )}
        <LinkButton href={allInstancesLink} title="View all instances across all DAG runs">List Instances, all runs</LinkButton>
        <LinkButton href={filterUpstreamLink}>Filter Upstream</LinkButton>
      </Flex>
      <Divider mt={3} />
    </>
  );
};

export default Nav;
