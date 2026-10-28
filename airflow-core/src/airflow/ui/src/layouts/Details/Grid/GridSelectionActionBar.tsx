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
import { Spinner } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { ActionBar } from "src/system-components";

import BulkClearTaskInstancesButton from "src/pages/TaskInstances/BulkClearTaskInstancesButton";
import BulkDeleteTaskInstancesButton from "src/pages/TaskInstances/BulkDeleteTaskInstancesButton";
import BulkMarkTaskInstancesAsButton from "src/pages/TaskInstances/BulkMarkTaskInstancesAsButton";

import type { GridSelection } from "src/context/gridSelection";
import { formatNumber } from "src/utils";

import { useGridSelectedTaskInstances } from "./useGridSelectedTaskInstances";
import { getCoveredNodeIds, type SelectableGroups } from "./useGridSelection";

type Props = {
  readonly clearSelection: VoidFunction;
  readonly dagId: string;
  readonly deselectCells: (keys: Array<string>) => void;
  readonly selectableGroups: SelectableGroups;
  readonly selection: GridSelection;
};

export const GridSelectionActionBar = ({
  clearSelection,
  dagId,
  deselectCells,
  selectableGroups,
  selection,
}: Props) => {
  const { i18n, t: translate } = useTranslation();
  const { data: selectedTaskInstances = [], isLoading } = useGridSelectedTaskInstances(
    dagId,
    selection,
    selectableGroups,
  );

  // Bulk actions report each updated task instance as `dag_id.run_id.task_id[map_index]`. A group cell
  // is deselected as soon as one of its task instances is updated.
  const deselectKeys = (keys: Array<string>) =>
    deselectCells(
      [...selection.entries()]
        .filter(([, cell]) =>
          getCoveredNodeIds(cell, selectableGroups).some((taskId) =>
            keys.some((key) => key.startsWith(`${dagId}.${cell.runId}.${taskId}[`)),
          ),
        )
        .map(([cellKey]) => cellKey),
    );

  return (
    <ActionBar.Root closeOnInteractOutside={false} open={selection.size > 0}>
      <ActionBar.Content>
        {isLoading ? (
          <Spinner size="sm" />
        ) : (
          <>
            <ActionBar.SelectionTrigger>
              {formatNumber(selectedTaskInstances.length, i18n.language)} {translate("selected")}
            </ActionBar.SelectionTrigger>
            <ActionBar.Separator />
            <BulkClearTaskInstancesButton
              clearSelections={clearSelection}
              selectedTaskInstances={selectedTaskInstances}
            />
            <BulkMarkTaskInstancesAsButton
              deselectKeys={deselectKeys}
              selectedTaskInstances={selectedTaskInstances}
            />
            <BulkDeleteTaskInstancesButton
              deselectKeys={deselectKeys}
              selectedTaskInstances={selectedTaskInstances}
            />
          </>
        )}
        <ActionBar.CloseTrigger onClick={clearSelection} />
      </ActionBar.Content>
    </ActionBar.Root>
  );
};
