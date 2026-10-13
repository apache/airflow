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
import { createListCollection, Flex, List, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiMinus, FiPlus } from "react-icons/fi";

import { IconButton, Select } from "src/system-components";

import { getMetaKey } from "src/utils";

import { ControlHelp } from "./ControlHelp";
import { DAG_RUN_LIMITS, type AggregationMode, type DagRunLimit, type TimeScale } from "./types";

const isDagRunLimit = (value: number): value is DagRunLimit => DAG_RUN_LIMITS.includes(value as DagRunLimit);
const AGGREGATION_HELP_OPTIONS = [
  { descriptionKey: "averageDurationHelp", labelKey: "averageDuration" },
  { descriptionKey: "fullTimeRangeHelp", labelKey: "fullTimeRange" },
  { descriptionKey: "shortestRunHelp", labelKey: "shortestRun" },
] as const;

type TimeScheduleViewControlsProps = {
  readonly aggregationMode: AggregationMode;
  readonly dagRunLimit: DagRunLimit;
  readonly onAggregationModeChange: (value: AggregationMode) => void;
  readonly onDagRunLimitChange: (value: DagRunLimit) => void;
  readonly onZoomIn: () => void;
  readonly onZoomOut: () => void;
  readonly timeScale: TimeScale;
  readonly zoomInDisabled: boolean;
  readonly zoomOutDisabled: boolean;
};

export const TimeScheduleViewControls = ({
  aggregationMode,
  dagRunLimit,
  onAggregationModeChange,
  onDagRunLimitChange,
  onZoomIn,
  onZoomOut,
  timeScale,
  zoomInDisabled,
  zoomOutDisabled,
}: TimeScheduleViewControlsProps) => {
  const { t: translate } = useTranslation();
  const metaKey = getMetaKey();
  const dagRunLimitOptions = createListCollection({
    items: DAG_RUN_LIMITS.map((value) => ({
      label: translate("timeSchedule.dagRunLimitOption", { count: value }),
      value: String(value),
    })),
  });
  const aggregationOptions = createListCollection({
    items: [
      { label: translate("timeSchedule.averageDuration"), value: "mean" },
      { label: translate("timeSchedule.fullTimeRange"), value: "max" },
      { label: translate("timeSchedule.shortestRun"), value: "min" },
    ],
  });

  return (
    <Flex
      align={{ base: "stretch", md: "center" }}
      flexDirection={{ base: "column", md: "row" }}
      flexShrink={0}
      gap={{ base: 2, md: 6 }}
      width={{ base: "100%", md: "auto" }}
    >
      <Flex align="center" gap={1}>
        <ControlHelp
          label={translate("timeSchedule.zoomHelpLabel")}
          title={translate("timeSchedule.zoomControls")}
        >
          <Text>{translate("timeSchedule.zoomHelp")}</Text>
          <Text>{translate("timeSchedule.zoomButtonHelp")}</Text>
          <List.Root gap={1} paddingStart={4}>
            <List.Item>{translate("timeSchedule.zoomMouseHelp", { metaKey })}</List.Item>
            <List.Item>{translate("timeSchedule.zoomKeyboardHelp", { metaKey })}</List.Item>
          </List.Root>
        </ControlHelp>
        <Flex align="center" gap={2}>
          <IconButton
            aria-label={translate("timeSchedule.zoomOut")}
            disabled={zoomOutDisabled}
            onClick={onZoomOut}
            size="sm"
            variant="outline"
          >
            <FiMinus />
          </IconButton>
          <IconButton
            aria-label={translate("timeSchedule.zoomIn")}
            disabled={zoomInDisabled}
            onClick={onZoomIn}
            size="sm"
            variant="outline"
          >
            <FiPlus />
          </IconButton>
          <Text color="fg.muted" fontSize="sm" minWidth="3rem" textAlign="center">
            {translate("timeSchedule.minutes", { value: timeScale })}
          </Text>
        </Flex>
      </Flex>
      <Flex align="center" gap={1} width={{ base: "100%", md: "auto" }}>
        <ControlHelp
          label={translate("timeSchedule.dagRunLimitHelpLabel")}
          title={translate("timeSchedule.dagRunLimit")}
        >
          <Text>{translate("timeSchedule.dagRunLimitHelp")}</Text>
        </ControlHelp>
        <Select.Root
          collection={dagRunLimitOptions}
          data-testid="time-schedule-dag-run-limit"
          flex={{ base: 1, md: "initial" }}
          onValueChange={({ value }) => {
            const [selectedValue] = value;

            const parsedValue = Number(selectedValue);

            if (isDagRunLimit(parsedValue)) {
              onDagRunLimitChange(parsedValue);
            }
          }}
          size="sm"
          value={[String(dagRunLimit)]}
          width={{ base: "auto", md: "9rem" }}
        >
          <Select.Trigger triggerProps={{ "aria-label": translate("timeSchedule.dagRunsToDisplay") }}>
            <Select.ValueText />
          </Select.Trigger>
          <Select.Content>
            {dagRunLimitOptions.items.map((option) => (
              <Select.Item item={option} key={option.value}>
                {option.label}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      </Flex>
      <Flex align="center" gap={1} width={{ base: "100%", md: "auto" }}>
        <ControlHelp
          label={translate("timeSchedule.durationAggregationHelpLabel")}
          title={translate("timeSchedule.durationAggregation")}
        >
          <Text>{translate("timeSchedule.durationAggregationHelp")}</Text>
          <List.Root gap={1.5} paddingStart={4}>
            {AGGREGATION_HELP_OPTIONS.map(({ descriptionKey, labelKey }) => (
              <List.Item key={labelKey}>
                <Text as="span">
                  <Text as="span" fontWeight="semibold">
                    {translate(`timeSchedule.${labelKey}`)}:
                  </Text>{" "}
                  {translate(`timeSchedule.${descriptionKey}`)}
                </Text>
              </List.Item>
            ))}
          </List.Root>
        </ControlHelp>
        <Select.Root
          collection={aggregationOptions}
          data-testid="time-schedule-aggregation"
          flex={{ base: 1, md: "initial" }}
          onValueChange={({ value }) => {
            const [selectedValue] = value;

            if (selectedValue === "mean" || selectedValue === "max" || selectedValue === "min") {
              onAggregationModeChange(selectedValue);
            }
          }}
          size="sm"
          value={[aggregationMode]}
          width={{ base: "auto", md: "11rem" }}
        >
          <Select.Trigger triggerProps={{ "aria-label": translate("timeSchedule.durationAggregation") }}>
            <Select.ValueText />
          </Select.Trigger>
          <Select.Content>
            {aggregationOptions.items.map((option) => (
              <Select.Item item={option} key={option.value}>
                {option.label}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      </Flex>
    </Flex>
  );
};
