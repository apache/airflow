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
import { useTaskUrlBuilder } from "src/hooks/useUrlBuilders";

type Props = {
  readonly id: string;
} & TaskNameProps;

export const TaskLink = forwardRef<HTMLAnchorElement, Props>(({ id, isGroup, isMapped, ...rest }, ref) => {
  const { runId } = useParams();
  const [searchParams] = useSearchParams();
  const buildTaskUrl = useTaskUrlBuilder();

  const taskPath = buildTaskUrl({
    isGroup,
    isMapped,
    runId,
    taskId: id,
  });

  return (
    <RouterLink ref={ref} to={{ pathname: taskPath, search: searchParams.toString() }}>
      <TaskName isGroup={isGroup} isMapped={isMapped} {...rest} />
    </RouterLink>
  );
});
