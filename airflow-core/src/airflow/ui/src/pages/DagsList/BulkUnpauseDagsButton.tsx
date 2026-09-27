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

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import { ActionErrors } from "src/components/ActionErrors";
import { ConfirmationModal } from "src/components/ConfirmationModal";

import { useBulkSetDagSchedulingState } from "src/queries/useBulkSetDagSchedulingState";

type Props = {
  readonly deselectKeys: (keys: Array<string>) => void;
  readonly selectedDags: Array<DAGWithLatestDagRunsResponse>;
};

const BulkUnpauseDagsButton = ({ deselectKeys, selectedDags }: Props) => {
  const { t: translate } = useTranslation(["common", "dags"]);
  const { onClose, onOpen, open } = useDisclosure();
  const { bulkAction, data, error, isPending, reset } = useBulkSetDagSchedulingState({
    deselectKeys,
    onSuccessConfirm: onClose,
  });

  const displayName = `${selectedDags.length} ${translate("dag", { count: selectedDags.length })}`;

  const handleOpen = () => {
    reset();
    onOpen();
  };

  // Unlike the single-Dag toggle, always confirm: unpausing many Dags at once can trigger
  // a burst of catchup runs.
  return (
    <>
      <Button
        data-testid="bulk-unpause-dags"
        disabled={selectedDags.every((dag) => !dag.is_paused && dag.scheduling_state !== "draining")}
        loading={isPending}
        onClick={handleOpen}
        variant="outline"
      >
        {translate("common:unpause")}
      </Button>
      <ConfirmationModal
        header={`${translate("common:unpause")} ${displayName}?`}
        onConfirm={() =>
          bulkAction({
            actions: [
              {
                action: "update",
                action_on_non_existence: "skip",
                // Draining Dags go back to active too, the same as clicking a draining Dag's toggle.
                entities: selectedDags.map((dag) => ({ dag_id: dag.dag_id, scheduling_state: "active" })),
              },
            ],
          })
        }
        onOpenChange={onClose}
        open={open}
      >
        <ActionErrors actionResponse={data?.update} error={error} />
      </ConfirmationModal>
    </>
  );
};

export default BulkUnpauseDagsButton;
