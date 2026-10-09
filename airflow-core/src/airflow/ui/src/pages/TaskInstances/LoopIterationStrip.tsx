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
import { Box, Button, createListCollection, HStack, Icon, Text, Wrap } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdLoop } from "react-icons/md";
import { useParams, useSearchParams } from "react-router-dom";

import { Select } from "src/system-components";

import { DurationCell } from "src/components/DurationCell";
import { LoopOptionLabel } from "src/components/LoopOptionLabel";
import { StateBadge } from "src/components/StateBadge";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useLoopFilterOptions, type LoopOption } from "src/queries/useLoopFilterOptions";

/**
 * Every pass of every loop in the run, as chips above the table.
 *
 * The loop filter can express all of this, but only once you open it. A loop's shape -- how many
 * passes it took and how they went -- is the thing you came to the page for, so it is on the page
 * rather than inside a menu, and each chip is also the control that narrows to it.
 */
// Past this many passes the chips stop fitting, so they collapse into the same rich dropdown
// the filter uses -- the threshold task tries already settled on.
const ITERATION_DROPDOWN_LIMIT = 10;

export const LoopIterationStrip = () => {
  const { t: translate } = useTranslation(["common"]);
  const { dagId, groupId, runId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const { options } = useLoopFilterOptions({ dagId, groupId, runId });

  if (options.length === 0) {
    return undefined;
  }

  const activeLoop = searchParams.get(SearchParamsKeys.LOOP_ID);
  const activeIteration = searchParams.get(SearchParamsKeys.ITERATION);

  const isActive = (option: LoopOption) =>
    option.loopId === activeLoop &&
    (option.iteration === undefined
      ? activeIteration === null || activeIteration === ""
      : String(option.iteration) === activeIteration);

  const select = (option: LoopOption) => {
    const next = new URLSearchParams(searchParams);

    // Clicking the chip that is already on clears it, so the strip toggles rather than
    // trapping you in a filter you have to go to the pill to remove.
    if (isActive(option)) {
      next.delete(SearchParamsKeys.LOOP_ID);
      next.delete(SearchParamsKeys.ITERATION);
    } else {
      next.set(SearchParamsKeys.LOOP_ID, option.loopId);
      if (option.iteration === undefined) {
        next.delete(SearchParamsKeys.ITERATION);
      } else {
        next.set(SearchParamsKeys.ITERATION, String(option.iteration));
      }
    }
    setSearchParams(next);
  };

  const loopIds = [...new Set(options.map((option) => option.loopId))];

  return (
    <Box mb={3}>
      {loopIds.map((loopId) => {
        const forLoop = options.filter((option) => option.loopId === loopId);
        const [wholeLoop] = forLoop;
        // The "All" entry rides along in forLoop; the threshold counts passes, as tries do.
        const passes = forLoop.length - 1;

        return (
          <HStack align="center" gap={2} key={loopId} mb={1} wrap="wrap">
            <HStack color="fg.muted" gap={1}>
              <Icon aria-hidden as={MdLoop} boxSize={3.5} />
              <Text fontSize="sm" fontWeight="medium">
                {loopId}
              </Text>
              {wholeLoop?.iterationsRan === undefined ? undefined : (
                <Text color={wholeLoop.state === "failed" ? "fg.error" : "fg.muted"} fontSize="xs">
                  {wholeLoop.iterationsRan}/{wholeLoop.maxIterations}
                </Text>
              )}
            </HStack>
            {passes > ITERATION_DROPDOWN_LIMIT ? (
              <Select.Root
                collection={createListCollection({
                  items: forLoop,
                  // LoopOption has no ``label``, which is what the default reads, so the
                  // trigger and typeahead would otherwise see every pass as blank.
                  itemToString: (option: LoopOption) =>
                    option.iteration === undefined
                      ? translate("common:filters.allIterations")
                      : String(option.iteration),
                })}
                onValueChange={({ value }) => {
                  const picked = forLoop.find((option) => option.value === value[0]);

                  if (picked !== undefined) {
                    select(picked);
                  }
                }}
                value={forLoop.filter(isActive).map((option) => option.value)}
                width="320px"
              >
                <Select.Trigger>
                  <Select.ValueText placeholder={translate("common:filters.allIterations")}>
                    {(picked: Array<LoopOption>) =>
                      picked[0] === undefined ? undefined : (
                        <LoopOptionLabel hideLoopName option={picked[0]} />
                      )
                    }
                  </Select.ValueText>
                </Select.Trigger>
                <Select.Content>
                  {forLoop.map((option) => (
                    <Select.Item item={option} key={option.value}>
                      <LoopOptionLabel hideLoopName option={option} />
                    </Select.Item>
                  ))}
                </Select.Content>
              </Select.Root>
            ) : (
              <Wrap gap={1}>
                {forLoop.map((option) => (
                  <Button
                    aria-pressed={isActive(option)}
                    key={option.value}
                    onClick={() => select(option)}
                    size="xs"
                    variant={isActive(option) ? "solid" : "outline"}
                  >
                    <HStack gap={1}>
                      <Text>{option.iteration ?? translate("common:filters.allIterations")}</Text>
                      {option.state === undefined ? undefined : <StateBadge state={option.state} />}
                      {option.duration === undefined ? undefined : (
                        <Text color="fg.muted" fontSize="2xs">
                          <DurationCell duration={option.duration} />
                        </Text>
                      )}
                    </HStack>
                  </Button>
                ))}
              </Wrap>
            )}
          </HStack>
        );
      })}
    </Box>
  );
};
