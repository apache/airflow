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
import { Button, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { DAGWithLatestDagRunsResponse, DagSchedulingState } from "openapi/requests/types.gen";

import { ActionErrors } from "src/components/ActionErrors";
import { ConfirmationModal } from "src/components/ConfirmationModal";
import { PauseOrDrainChoiceModal } from "src/components/PauseOrDrainChoiceModal";

import { useBulkSetDagSchedulingState } from "src/queries/useBulkSetDagSchedulingState";

type Props = {
  readonly deselectKeys: (keys: Array<string>) => void;
  readonly selectedDags: Array<DAGWithLatestDagRunsResponse>;
};

const BulkPauseDrainDagsButton = ({ deselectKeys, selectedDags }: Props) => {
  const { t: translate } = useTranslation(["common", "dags"]);
  const { onClose, onOpen, open } = useDisclosure();
  const { bulkAction, data, error, isPending, reset } = useBulkSetDagSchedulingState({
    deselectKeys,
    onSuccessConfirm: onClose,
  });

  // Nothing is running in any selected unpaused Dag, so draining and pausing now are equivalent
  // for the whole batch — skip the drain-vs-pause choice, same as the single-Dag toggle does.
  // Already-paused Dags are never drained (see runBulkAction), so their runs don't count.
  const allIdle = selectedDags.every((dag) => dag.is_paused || !dag.has_unfinished_runs);
  const displayName = `${selectedDags.length} ${translate("dag", { count: selectedDags.length })}`;

  const runBulkAction = (schedulingState: DagSchedulingState) => {
    bulkAction({
      actions: [
        {
          action: "update",
          action_on_non_existence: "skip",
          entities: selectedDags.map((dag) => ({
            dag_id: dag.dag_id,
            // Draining a paused Dag would unpause it and let the scheduler resume its unfinished runs.
            scheduling_state: dag.is_paused ? "paused" : schedulingState,
          })),
        },
      ],
    });
  };

  const handleOpen = () => {
    reset();
    onOpen();
  };

  return (
    <>
      <Button
        data-testid="bulk-pause-drain-dags"
        disabled={selectedDags.every((dag) => dag.is_paused)}
        loading={isPending}
        onClick={handleOpen}
        variant="outline"
      >
        {translate("dags:schedulingActions.pauseSelected")}
      </Button>
      {allIdle ? (
        <ConfirmationModal
          header={`${translate("common:pause")} ${displayName}?`}
          onConfirm={() => runBulkAction("paused")}
          onOpenChange={onClose}
          open={open}
        >
          <ActionErrors actionResponse={data?.update} error={error} />
        </ConfirmationModal>
      ) : (
        <PauseOrDrainChoiceModal
          displayName={displayName}
          onChooseDrain={() => runBulkAction("draining")}
          onChoosePause={() => runBulkAction("paused")}
          onOpenChange={onClose}
          open={open}
        >
          <ActionErrors actionResponse={data?.update} error={error} />
        </PauseOrDrainChoiceModal>
      )}
    </>
  );
};

export default BulkPauseDrainDagsButton;
