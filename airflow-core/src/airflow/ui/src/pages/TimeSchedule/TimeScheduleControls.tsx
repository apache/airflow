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
import { Box, Flex } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { ButtonGroupToggle } from "src/system-components";

import { FilterBar } from "src/components/FilterBar";

import type { ViewMode } from "./types";

type TimeScheduleControlsProps = {
  readonly filterConfigs: Parameters<typeof FilterBar>[0]["configs"];
  readonly initialValues: Parameters<typeof FilterBar>[0]["initialValues"];
  readonly onFiltersChange: Parameters<typeof FilterBar>[0]["onFiltersChange"];
  readonly onViewModeChange: (value: ViewMode) => void;
  readonly viewMode: ViewMode;
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
      <ButtonGroupToggle<ViewMode>
        data-testid="time-schedule-view-mode"
        marginStart="auto"
        onChange={onViewModeChange}
        options={[
          { label: translate("timeSchedule.day"), value: "day" },
          { label: translate("timeSchedule.week"), value: "week" },
        ]}
        size="sm"
        value={viewMode}
      />
    </Flex>
  );
};
