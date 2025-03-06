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
import { Link } from "@chakra-ui/react";
import { useParams, useSearchParams, Link as RouterLink } from "react-router-dom";

import { TaskName, type TaskNameProps } from "src/components/TaskName";

type Props = {
  readonly id: string;
} & TaskNameProps;

export const TaskLink = ({ id, isGroup, isMapped, ...rest }: Props) => {
  const { dagId = "", runId, taskId } = useParams();
  const [searchParams] = useSearchParams();

  // We don't have a task group details page to link to
  if (isGroup) {
    return <TaskName isGroup={true} isMapped={isMapped} {...rest} />;
  }

  return (
    <Link asChild data-testid={id}>
      <RouterLink
        to={{
          // Do not include runId if there is no selected run, clicking a second time will deselect a task id
          pathname: `/dags/${dagId}/${runId === undefined ? "" : `runs/${runId}/`}${taskId === id ? "" : `tasks/${id}`}${isMapped && taskId !== id && runId !== undefined ? "/mapped" : ""}`,
          search: searchParams.toString(),
        }}
      >
        <TaskName isMapped={isMapped} {...rest} />
      </RouterLink>
    </Link>
  );
};
