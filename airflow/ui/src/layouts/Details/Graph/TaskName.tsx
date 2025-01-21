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
import { type LinkProps, Link, Text } from "@chakra-ui/react";
import type { CSSProperties } from "react";
import { FiArrowUpRight, FiArrowDownRight } from "react-icons/fi";
import { useParams, useSearchParams, Link as RouterLink } from "react-router-dom";

import type { NodeResponse } from "openapi/requests/types.gen";

type Props = {
  readonly id: string;
  readonly isGroup?: boolean;
  readonly isMapped?: boolean;
  readonly isOpen?: boolean;
  readonly isZoomedOut?: boolean;
  readonly label: string;
  readonly setupTeardownType?: NodeResponse["setup_teardown_type"];
} & LinkProps;

const iconStyle: CSSProperties = {
  display: "inline",
  position: "relative",
  verticalAlign: "middle",
};

export const TaskName = ({
  id,
  isGroup = false,
  isMapped = false,
  isOpen = false,
  isZoomedOut,
  label,
  setupTeardownType,
  ...rest
}: Props) => {
  const { dagId = "", runId, taskId } = useParams();
  const [searchParams] = useSearchParams();

  // We don't have a task group details page to link to
  if (isGroup) {
    return (
      <Text fontSize="md" fontWeight="bold">
        {label}
      </Text>
    );
  }

  return (
    <Link asChild data-testid={id} fontSize={isZoomedOut ? "lg" : "md"} fontWeight="bold" {...rest}>
      <RouterLink
        to={{
          // Do not include runId if there is no selected run, clicking a second time will deselect a task id
          pathname: `/dags/${dagId}/${runId === undefined ? "" : `runs/${runId}/`}${taskId === id ? "" : `tasks/${id}`}`,
          search: searchParams.toString(),
        }}
      >
        {label}
        {isMapped ? " [ ]" : undefined}
        {setupTeardownType === "setup" && <FiArrowUpRight size={isZoomedOut ? 24 : 15} style={iconStyle} />}
        {setupTeardownType === "teardown" && (
          <FiArrowDownRight size={isZoomedOut ? 24 : 15} style={iconStyle} />
        )}
      </RouterLink>
    </Link>
  );
};
