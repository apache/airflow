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
import type { ReactNode } from "react";

import { Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { useLoopGroupNode } from "src/queries/useIsLoopGroup";

export const useLoopRuleStats = (
  groupId: string | undefined,
): Array<{ label: string; value: ReactNode | string }> => {
  const { t: translate } = useTranslation("dag");
  const node = useLoopGroupNode(groupId ?? "");

  if (groupId === undefined || node?.is_loop !== true) {
    return [];
  }

  const criteria = node.loop_exit_task_id?.split(".").at(-1);

  return [
    {
      label: translate("loop.rules.behaviour"),
      value:
        node.loop_exit_criteria_doc ??
        (criteria === undefined
          ? translate("loop.rules.boundedFor")
          : translate("loop.rules.whileUntil", { criteria })),
    },
    { label: translate("loop.rules.maxIterations"), value: node.loop_max_iterations ?? "" },
    ...(node.loop_exit_task_id === null || node.loop_exit_task_id === undefined
      ? []
      : [
          {
            label: translate("loop.rules.exitCriteria"),
            value: (
              <Text fontFamily="mono" fontSize="sm">
                {node.loop_exit_task_id}
              </Text>
            ),
          },
        ]),
  ];
};
