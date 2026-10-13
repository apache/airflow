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

import { HStack, Icon, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiRepeat } from "react-icons/fi";

import { useLoopSummary } from "src/queries/useLoopSummary";

import { reasonSentence } from "./LoopIterations/loopUtils";

export const useLoopOutcomeStats = ({
  dagId,
  groupId,
  runId,
}: {
  dagId: string;
  groupId: string;
  runId: string;
}): Array<{ label: string; value: ReactNode | string }> => {
  const { t: translate } = useTranslation("dag");
  const { data: summary } = useLoopSummary({ dagId, groupId, runId });

  if (summary === undefined) {
    return [];
  }

  return [
    {
      label: translate("loop.outcome"),
      value: (
        <Text color={summary.status === "failed" ? "fg.error" : undefined}>
          {reasonSentence(translate, summary)}
        </Text>
      ),
    },
    {
      label: translate("loop.iterations"),
      value: (
        <HStack gap={1}>
          <Icon aria-hidden as={FiRepeat} boxSize={3.5} />
          <Text>
            {summary.iterations_ran}/{summary.max_iterations}
          </Text>
        </HStack>
      ),
    },
    {
      label: translate("loop.rules.behaviour"),
      value:
        summary.exit_criteria_doc ??
        (summary.exit_criteria_name === null || summary.exit_criteria_name === undefined
          ? translate("loop.rules.boundedFor")
          : translate("loop.rules.whileUntil", { criteria: summary.exit_criteria_name })),
    },
  ];
};
