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
import { Link as ChakraLink, Table, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { LoopDecisionReason } from "src/components/LoopDecisionReason";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useLoopSummary } from "src/queries/useLoopSummary";
import { useDurationFormat } from "src/utils";

import { reasonSentence } from "./loopUtils";

/**
 * Every pass the loop ran, with the gate's reason for each.
 *
 * The header reports only the loop's final outcome, and the iteration switcher only labels a pass,
 * so this is where a reason per pass is readable -- which is what you need to see why a loop ran
 * as long as it did.
 */
export const LoopIterations = () => {
  const { dagId = "", groupId = "", runId = "" } = useParams();
  // "dag" leads so reasonSentence's unprefixed loop.* keys resolve; common keys are prefixed.
  const { t: translate } = useTranslation(["dag", "common"]);
  const { formatElapsed } = useDurationFormat();
  const [searchParams] = useSearchParams();
  const { data: summary } = useLoopSummary({ dagId, groupId, runId });

  const iterations = summary?.iterations ?? [];

  // Picking a pass scopes the group's task instances to it, the same move the breadcrumb's
  // iteration switcher makes. A coordinate addresses one row and this names a whole pass, so the
  // old one is dropped rather than sending you to a row that pass may not have; paging restarts
  // because a cursor into one pass means nothing in another.
  const iterationSearch = (index: number) => {
    const next = new URLSearchParams(searchParams);

    next.set(SearchParamsKeys.LOOP_ID, groupId);
    next.set(SearchParamsKeys.ITERATION, String(index));
    next.delete(SearchParamsKeys.REGION_ID);
    next.delete(SearchParamsKeys.REGION_INDEX);
    next.delete(SearchParamsKeys.CURSOR);
    next.delete(SearchParamsKeys.OFFSET);

    return next.toString();
  };

  if (iterations.length === 0) {
    return (
      <Text color="fg.muted" py={4}>
        {translate("common:noItemsFound", { modelName: translate("loop.iterations") })}
      </Text>
    );
  }

  // The pass that ended the loop, whichever way it ended; its row carries the outcome sentence.
  const endedAt = summary?.failed_at_iteration ?? summary?.stopped_at_iteration;
  // Newest first: the pass you came to read about is the one that just ran, and a loop near its
  // limit would otherwise bury it. The summary counts them up, so this reverses a copy.
  const newestFirst = [...iterations].reverse();

  return (
    <Table.Root minW="max-content" striped>
      <Table.Header>
        <Table.Row>
          <Table.ColumnHeader w="110px">{translate("common:filters.iteration")}</Table.ColumnHeader>
          <Table.ColumnHeader w="120px">{translate("common:state")}</Table.ColumnHeader>
          <Table.ColumnHeader w="170px">{translate("common:startDate")}</Table.ColumnHeader>
          <Table.ColumnHeader w="110px">{translate("common:duration")}</Table.ColumnHeader>
          <Table.ColumnHeader w="90px">{translate("common:task_other")}</Table.ColumnHeader>
          <Table.ColumnHeader>{translate("common:taskInstance.loopDecision")}</Table.ColumnHeader>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {newestFirst.map((iteration) => {
          const ended = iteration.index === endedAt;

          return (
            <Table.Row
              key={iteration.index}
              title={ended && summary ? reasonSentence(translate, summary) : undefined}
            >
              <Table.Cell>
                <ChakraLink asChild color="fg.info">
                  {/* ".." is the group's task instances tab, this tab's sibling. */}
                  <RouterLink
                    aria-label={translate("loop.iterationLabel", { index: iteration.index })}
                    relative="path"
                    to={{ pathname: "..", search: iterationSearch(iteration.index) }}
                  >
                    {iteration.index}
                  </RouterLink>
                </ChakraLink>
              </Table.Cell>
              <Table.Cell>
                {iteration.state === null || iteration.state === undefined ? undefined : (
                  <StateBadge fontSize="xs" state={iteration.state}>
                    {translate(`common:states.${iteration.state}`)}
                  </StateBadge>
                )}
              </Table.Cell>
              <Table.Cell>
                <Time datetime={iteration.start_date} />
              </Table.Cell>
              <Table.Cell>{formatElapsed(iteration.start_date, iteration.end_date)}</Table.Cell>
              <Table.Cell>{iteration.task_count}</Table.Cell>
              <Table.Cell>
                {iteration.decision_reason === undefined ? undefined : (
                  <LoopDecisionReason reason={iteration.decision_reason} />
                )}
              </Table.Cell>
            </Table.Row>
          );
        })}
      </Table.Body>
    </Table.Root>
  );
};
