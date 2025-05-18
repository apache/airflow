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
import { forwardRef } from "react";
import { useParams, useSearchParams, Link as RouterLink } from "react-router-dom";

import { TaskName, type TaskNameProps } from "src/components/TaskName";

type Props = {
  readonly id: string;
} & TaskNameProps;

export const TaskLink = forwardRef<HTMLAnchorElement, Props>(({ id, isGroup, isMapped, ...rest }, ref) => {
  const { dagId = "", groupId, runId, taskId } = useParams();
  const [searchParams] = useSearchParams();

  const basePath = `/dags/${dagId}${runId === undefined ? "" : `/runs/${runId}`}`;
  const taskPath = isGroup
    ? groupId === id
      ? ""
      : `/tasks/group/${id}`
    : taskId === id
      ? ""
      : `/tasks/${id}${isMapped && taskId !== id && runId !== undefined ? "/mapped" : ""}`;

  return (
    <RouterLink ref={ref} to={{ pathname: basePath + taskPath, search: searchParams.toString() }}>
      <TaskName isGroup={isGroup} isMapped={isMapped} {...rest} />
    </RouterLink>
  );
});
