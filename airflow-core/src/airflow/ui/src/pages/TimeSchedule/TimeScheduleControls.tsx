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
import { Box, Button, createListCollection, Flex, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { FilterBar } from "src/components/FilterBar";
import { Checkbox } from "src/components/ui/Checkbox";
import { Select } from "src/components/ui/Select";

import {
  DAG_RUN_LIMITS,
  type AggregationMode,
  type DagRunLimit,
  type TimeScale,
  type ViewMode,
} from "./types";

const isDagRunLimit = (value: number): value is DagRunLimit => DAG_RUN_LIMITS.includes(value as DagRunLimit);

type TimeScheduleControlsProps = {
  readonly filterConfigs: Parameters<typeof FilterBar>[0]["configs"];
  readonly initialValues: Parameters<typeof FilterBar>[0]["initialValues"];
  readonly onFiltersChange: Parameters<typeof FilterBar>[0]["onFiltersChange"];
};

type TimeScheduleViewControlsProps = {
  readonly aggregationMode: AggregationMode;
  readonly dagRunLimit: DagRunLimit;
  readonly onAggregationModeChange: (value: AggregationMode) => void;
  readonly onDagRunLimitChange: (value: DagRunLimit) => void;
  readonly onScheduledOnlyChange: (checked: boolean) => void;
  readonly onViewModeChange: (value: ViewMode) => void;
  readonly onZoomIn: () => void;
  readonly onZoomOut: () => void;
  readonly showScheduledOnly: boolean;
  readonly timeScale: TimeScale;
  readonly viewMode: ViewMode;
  readonly zoomInDisabled: boolean;
  readonly zoomOutDisabled: boolean;
};

export const TimeScheduleControls = ({
  filterConfigs,
  initialValues,
  onFiltersChange,
}: TimeScheduleControlsProps) => (
  <Flex align="flex-start" gap={4} justify="space-between" wrap="wrap">
    <Box flex="0 1 auto" maxW="100%" width="fit-content">
      <FilterBar
        configs={filterConfigs}
        initialValues={initialValues}
        onFiltersChange={onFiltersChange}
        showPresetFilters={false}
      />
    </Box>
  </Flex>
);

export const TimeScheduleViewControls = ({
  aggregationMode,
  dagRunLimit,
  onAggregationModeChange,
  onDagRunLimitChange,
  onScheduledOnlyChange,
  onViewModeChange,
  onZoomIn,
  onZoomOut,
  showScheduledOnly,
  timeScale,
  viewMode,
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
  const viewModeOptions = createListCollection({
    items: [
      { label: translate("timeSchedule.day"), value: "day" },
      { label: translate("timeSchedule.week"), value: "week" },
    ],
  });
  const aggregationOptions = createListCollection({
    items: [
      { label: translate("timeSchedule.mean"), value: "mean" },
      { label: translate("timeSchedule.max"), value: "max" },
      { label: translate("timeSchedule.min"), value: "min" },
    ],
  });

  return (
    <Flex align="center" flexShrink={0} gap={6} wrap="nowrap">
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
      <Select.Root
        collection={dagRunLimitOptions}
        data-testid="time-schedule-dag-run-limit"
        onValueChange={({ value }) => {
          const [selectedValue] = value;

          const parsedValue = Number(selectedValue);

          if (isDagRunLimit(parsedValue)) {
            onDagRunLimitChange(parsedValue);
          }
        }}
        size="sm"
        value={[String(dagRunLimit)]}
        width="150px"
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
      <Select.Root
        collection={viewModeOptions}
        data-testid="time-schedule-view-mode"
        onValueChange={({ value }) => {
          const [selectedValue] = value;

          if (selectedValue === "day" || selectedValue === "week") {
            onViewModeChange(selectedValue);
          }
        }}
        size="sm"
        value={[viewMode]}
        width="100px"
      >
        <Select.Trigger triggerProps={{ "aria-label": translate("timeSchedule.viewMode") }}>
          <Select.ValueText />
        </Select.Trigger>
        <Select.Content>
          {viewModeOptions.items.map((option) => (
            <Select.Item item={option} key={option.value}>
              {option.label}
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Root>
      <Select.Root
        collection={aggregationOptions}
        data-testid="time-schedule-aggregation"
        onValueChange={({ value }) => {
          const [selectedValue] = value;

          if (selectedValue === "mean" || selectedValue === "max" || selectedValue === "min") {
            onAggregationModeChange(selectedValue);
          }
        }}
        size="sm"
        value={[aggregationMode]}
        width="100px"
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
      <Checkbox
        checked={showScheduledOnly}
        inputProps={{ onChange: (event) => onScheduledOnlyChange(event.target.checked) }}
      >
        {translate("timeSchedule.scheduledDagsOnly")}
      </Checkbox>
    </Flex>
  );
};
