/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may not obtain a copy of the License at
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
import { Box, Button, HStack, Select, Spinner, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiClock } from "react-icons/fi";

import { useRecentConfigurations } from "src/queries/useRecentConfigurations";
import type { RecentConfiguration } from "src/queries/useRecentConfigurations";

interface RecentConfigurationsDropdownProps {
  dagId: string;
  onConfigSelect: (config: RecentConfiguration | null) => void;
  selectedConfig: RecentConfiguration | null;
}

export const RecentConfigurationsDropdown = ({
  dagId,
  onConfigSelect,
  selectedConfig,
}: RecentConfigurationsDropdownProps) => {
  const { t: translate } = useTranslation(["common", "components"]);
  
  const { data, isLoading, error } = useRecentConfigurations(dagId);

  const handleConfigSelect = (runId: string) => {
    const config = data?.configurations.find(c => c.run_id === runId) || null;
    onConfigSelect(config);
  };

  const handleClearSelection = () => {
    onConfigSelect(null);
  };

  if (error) {
    return (
      <Box>
        <Text color="fg.error" fontSize="sm">
          {translate("components:triggerDag.recentConfigs.error")}
        </Text>
      </Box>
    );
  }

  if (isLoading) {
    return (
      <HStack>
        <Spinner size="sm" />
        <Text fontSize="sm">{translate("components:triggerDag.recentConfigs.loading")}</Text>
      </HStack>
    );
  }

  if (!data?.configurations || data.configurations.length === 0) {
    return (
      <Box>
        <Text fontSize="sm" color="fg.muted">
          {translate("components:triggerDag.recentConfigs.noConfigs")}
        </Text>
      </Box>
    );
  }

  return (
    <div>
      <div>
        {translate("components:triggerDag.recentConfigs.label")}
      </div>
      <div>
        <select
          placeholder={translate("components:triggerDag.recentConfigs.placeholder")}
          value={selectedConfig?.run_id || ""}
          onChange={(e) => {
            if (e.target.value) {
              handleConfigSelect(e.target.value);
            } else {
              handleClearSelection();
            }
          }}
        >
          {data.configurations.map((config) => (
            <option key={config.run_id} value={config.run_id}>
              {config.run_id} - {config.logical_date ? config.logical_date.toString() : "N/A"}
            </option>
          ))}
        </select>
        {selectedConfig && (
          <button
            onClick={handleClearSelection}
          >
            {translate("components:triggerDag.recentConfigs.clear")}
          </button>
        )}
      </div>
      <div>
        {translate("components:triggerDag.recentConfigs.help")}
      </div>
    </div>
  );
};
