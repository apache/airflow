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
import { Link as RouterLink } from "react-router-dom";

import type { DagScheduleAssetReference, TaskOutletAssetReference } from "openapi/requests/types.gen";
import { Button, Popover } from "src/components/ui";
import { pluralize } from "src/utils";

type Props = {
  readonly dependencies: Array<DagScheduleAssetReference | TaskOutletAssetReference>;
  readonly type: "Dag" | "Task";
};

export const DependencyPopover = ({ dependencies, type }: Props) => (
  // eslint-disable-next-line jsx-a11y/no-autofocus
  <Popover.Root autoFocus={false} lazyMount unmountOnExit>
    <Popover.Trigger asChild>
      <Button size="sm" variant="outline">
        {pluralize(type, dependencies.length)}
      </Button>
    </Popover.Trigger>
    <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
      <Popover.Arrow />
      <Popover.Body>
        {dependencies.map((dependency) => {
          let key = dependency.dag_id;
          let link = `/dags/${dependency.dag_id}`;
          let label = dependency.dag_id;

          if (type === "Task") {
            const dep = dependency as TaskOutletAssetReference;

            key = `${dep.dag_id}-${dep.task_id}`;
            link = `/dags/${dep.dag_id}/tasks/${dep.task_id}`;
            label = `${dep.dag_id}.${dep.task_id}`;
          }

          return (
            <Link asChild color="fg.info" display="block" key={key} py={2}>
              <RouterLink to={link}>{label}</RouterLink>
            </Link>
          );
        })}
      </Popover.Body>
    </Popover.Content>
  </Popover.Root>
);
