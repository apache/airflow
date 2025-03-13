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
import { Text } from "@chakra-ui/react";
import { FiCalendar } from "react-icons/fi";

import type { DAGDetailsResponse, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";
import { AssetExpression, type ExpressionType } from "src/components/AssetExpression";
import { Button, Popover, Tooltip } from "src/components/ui";

type Props = {
  readonly dag: DAGDetailsResponse | DAGWithLatestDagRunsResponse;
};

export const Schedule = ({ dag }: Props) =>
  Boolean(dag.timetable_summary) ? (
    dag.asset_expression === null ? (
      <Tooltip content={dag.timetable_description}>
        <Text fontSize="sm">
          <FiCalendar style={{ display: "inline" }} /> {dag.timetable_summary}
        </Text>
      </Tooltip>
    ) : (
      // eslint-disable-next-line jsx-a11y/no-autofocus
      <Popover.Root autoFocus={false} lazyMount positioning={{ placement: "bottom-end" }} unmountOnExit>
        <Popover.Trigger asChild>
          <Button size="sm" variant="ghost">
            <FiCalendar style={{ display: "inline" }} /> {dag.timetable_summary}
          </Button>
        </Popover.Trigger>
        <Popover.Content width="fit-content">
          <Popover.Arrow />
          <Popover.Body>
            <AssetExpression expression={dag.asset_expression as ExpressionType} />
          </Popover.Body>
        </Popover.Content>
      </Popover.Root>
    )
  ) : undefined;
