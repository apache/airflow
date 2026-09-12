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

import { Box, Button, createListCollection, Flex, Tabs, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiInfo } from "react-icons/fi";

import { Checkbox, IconButton, Select, Tooltip } from "src/system-components";

import { FilterBar } from "src/components/FilterBar";

import { TIMELINE_TOOLTIP_CONTENT_PROPS } from "./constants";
import {
  DAG_RUN_LIMITS,
  type AggregationMode,
  type DagRunLimit,
  type TimeScale,
  type ViewMode,
} from "./types";

const isDagRunLimit = (value: number): value is DagRunLimit => DAG_RUN_LIMITS.includes(value as DagRunLimit);
const AGGREGATION_HELP_OPTIONS = [
  { descriptionKey: "averageDurationHelp", labelKey: "averageDuration" },
  { descriptionKey: "fullTimeRangeHelp", labelKey: "fullTimeRange" },
  { descriptionKey: "shortestRunHelp", labelKey: "shortestRun" },
] as const;

type ControlHelpProps = {
  readonly children: ReactNode;
  readonly label: string;
  readonly title: string;
};

const ControlHelp = ({ children, label, title }: ControlHelpProps) => (
  <Tooltip
    content={
      <VStack align="start" gap={1.5} maxWidth="xs">
        <Text color="inherit" fontSize="sm" fontWeight="semibold">
          {title}
        </Text>
        {children}
      </VStack>
    }
    contentProps={TIMELINE_TOOLTIP_CONTENT_PROPS}
    openDelay={0}
    portalled
  >
    <IconButton aria-label={label} size="sm">
      <FiInfo />
    </IconButton>
  </Tooltip>
);

type TimeScheduleControlsProps = {
  readonly filterConfigs: Parameters<typeof FilterBar>[0]["configs"];
  readonly initialValues: Parameters<typeof FilterBar>[0]["initialValues"];
  readonly onFiltersChange: Parameters<typeof FilterBar>[0]["onFiltersChange"];
  readonly onViewModeChange: (value: ViewMode) => void;
  readonly viewMode: ViewMode;
};

type TimeScheduleViewControlsProps = {
  readonly aggregationMode: AggregationMode;
  readonly dagRunLimit: DagRunLimit;
  readonly onAggregationModeChange: (value: AggregationMode) => void;
  readonly onDagRunLimitChange: (value: DagRunLimit) => void;
  readonly onScheduledOnlyChange: (checked: boolean) => void;
  readonly onZoomIn: () => void;
  readonly onZoomOut: () => void;
  readonly showScheduledOnly: boolean;
  readonly timeScale: TimeScale;
  readonly zoomInDisabled: boolean;
  readonly zoomOutDisabled: boolean;
};

export const TimeScheduleControls = ({
  filterConfigs,
  initialValues,
  onFiltersChange,
  onViewModeChange,
  viewMode,
}: TimeScheduleControlsProps) => {
  const { t: translate } = useTranslation();

  return (
    <Flex align="center" gap={4} justify="space-between" wrap="wrap">
      <Box flex="0 1 auto" maxW="100%" width="fit-content">
        <FilterBar
          configs={filterConfigs}
          initialValues={initialValues}
          onFiltersChange={onFiltersChange}
          showPresetFilters={false}
        />
      </Box>
      <Box flexShrink={0} marginStart="auto">
        <Tabs.Root
          data-testid="time-schedule-view-mode"
          onValueChange={({ value }) => {
            if (value === "day" || value === "week") {
              onViewModeChange(value);
            }
          }}
          size="sm"
          value={viewMode}
          variant="line"
        >
          <Tabs.List
            bg="bg.muted"
            borderColor="border.subtle"
            borderRadius="md"
            borderWidth="1px"
            gap={1}
            p={1}
          >
            {(["day", "week"] as const).map((mode) => (
              <Tabs.Trigger
                _hover={{ bg: "bg.subtle", color: "fg" }}
                _selected={{ bg: "brand.muted", color: "fg", fontWeight: "bold" }}
                borderRadius="sm"
                color="fg.muted"
                fontSize="md"
                fontWeight="medium"
                height="32px"
                justifyContent="center"
                key={mode}
                value={mode}
                width="88px"
              >
                {translate(`timeSchedule.${mode}`)}
              </Tabs.Trigger>
            ))}
          </Tabs.List>
        </Tabs.Root>
      </Box>
    </Flex>
  );
};

export const TimeScheduleViewControls = ({
  aggregationMode,
  dagRunLimit,
  onAggregationModeChange,
  onDagRunLimitChange,
  onScheduledOnlyChange,
  onZoomIn,
  onZoomOut,
  showScheduledOnly,
  timeScale,
  zoomInDisabled,
  zoomOutDisabled,
}: TimeScheduleViewControlsProps) => {
  const { t: translate } = useTranslation();
  const dagRunLimitOptions = createListCollection({
    items: DAG_RUN_LIMITS.map((value) => ({
      label: translate("timeSchedule.latestDagRuns", { count: value }),
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
          <VStack align="stretch" as="ul" gap={1} listStyleType="none" margin={0} padding={0}>
            <Flex align="start" as="li" gap={1}>
              <Text aria-hidden="true">•</Text>
              <Text flex={1}>{translate("timeSchedule.zoomMouseHelp")}</Text>
            </Flex>
            <Flex align="start" as="li" gap={1}>
              <Text aria-hidden="true">•</Text>
              <Text flex={1}>{translate("timeSchedule.zoomKeyboardHelp")}</Text>
            </Flex>
          </VStack>
        </ControlHelp>
        <Flex align="center" gap={2}>
          <Button
            aria-label={translate("timeSchedule.zoomOut")}
            disabled={zoomOutDisabled}
            onClick={onZoomOut}
            size="sm"
            variant="outline"
          >
            −
          </Button>
          <Button
            aria-label={translate("timeSchedule.zoomIn")}
            disabled={zoomInDisabled}
            onClick={onZoomIn}
            size="sm"
            variant="outline"
          >
            +
          </Button>
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
          width={{ base: "auto", md: "150px" }}
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
          <VStack align="stretch" as="ul" gap={1.5} listStyleType="none" margin={0} padding={0}>
            {AGGREGATION_HELP_OPTIONS.map(({ descriptionKey, labelKey }) => (
              <Flex align="start" as="li" gap={1} key={labelKey}>
                <Text aria-hidden="true">•</Text>
                <Text flex={1}>
                  <Text as="span" fontWeight="semibold">
                    {translate(`timeSchedule.${labelKey}`)}:
                  </Text>{" "}
                  {translate(`timeSchedule.${descriptionKey}`)}
                </Text>
              </Flex>
            ))}
          </VStack>
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
          width={{ base: "auto", md: "175px" }}
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
      <Checkbox
        alignSelf={{ base: "start", md: "auto" }}
        checked={showScheduledOnly}
        inputProps={{ onChange: (event) => onScheduledOnlyChange(event.target.checked) }}
      >
        {translate("timeSchedule.scheduledDagsOnly")}
      </Checkbox>
    </Flex>
  );
};
