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
import { Box, useDisclosure } from "@chakra-ui/react";
import { useState } from "react";
import { FiRefreshCw } from "react-icons/fi";

import type { DAGRunResponse, TaskInstanceCollectionResponse } from "openapi/requests/types.gen";
import ActionButton from "src/components/ui/ActionButton";
import { useClearDagRun } from "src/queries/useClearRun";

import ClearRunDialog from "./ClearRunDialog";

type Props = {
  readonly dagRun: DAGRunResponse;
  readonly withText?: boolean;
};

const ClearRunButton = ({ dagRun, withText = true }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();

  const [affectedTasks, setAffectedTasks] = useState<TaskInstanceCollectionResponse>({
    task_instances: [],
    total_entries: 0,
  });

  const dagId = dagRun.dag_id;
  const dagRunId = dagRun.dag_run_id;

  const { isPending, mutate } = useClearDagRun({
    dagId,
    dagRunId,
    onSuccessConfirm: onClose,
    onSuccessDryRun: setAffectedTasks,
  });

  return (
    <Box>
      <ActionButton
        actionName="Clear Dag Run"
        icon={<FiRefreshCw />}
        onClick={onOpen}
        text="Clear Run"
        withText={withText}
      />

      <ClearRunDialog
        affectedTasks={affectedTasks}
        dagRun={dagRun}
        isPending={isPending}
        mutate={mutate}
        onClose={onClose}
        open={open}
      />
    </Box>
  );
};

export default ClearRunButton;
