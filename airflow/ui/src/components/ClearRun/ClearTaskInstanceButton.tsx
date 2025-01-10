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

import type { TaskInstanceCollectionResponse, TaskInstanceResponse } from "openapi/requests/types.gen";
import { useClearTaskInstances } from "src/queries/useClearTaskInstances";

import ActionButton from "../ui/ActionButton";
import ClearTaskInstanceDialog from "./ClearTaskInstanceDialog";

type Props = {
  readonly taskInstance: TaskInstanceResponse;
  readonly withText?: boolean;
};

const ClearTaskInstanceButton = ({ taskInstance, withText = true }: Props) => {
  const { onClose, onOpen, open } = useDisclosure();

  const [onlyFailed, setOnlyFailed] = useState(false);
  const onToggleOnlyFailed = () => setOnlyFailed((state) => !state);

  const [past, setPast] = useState(false);
  const onTogglePast = () => setPast((state) => !state);

  const [future, setFuture] = useState(false);
  const onToggleFuture = () => setFuture((state) => !state);

  const [upstream, setUpstream] = useState(false);
  const onToggleUpstream = () => setUpstream((state) => !state);

  const [downstream, setDownstream] = useState(false);
  const onToggleDownstream = () => setDownstream((state) => !state);

  const [affectedTasks, setAffectedTasks] = useState<TaskInstanceCollectionResponse>({
    task_instances: [],
    total_entries: 0,
  });

  const dagId = taskInstance.dag_id;
  const dagRunId = taskInstance.dag_run_id;

  const { isPending, mutate } = useClearTaskInstances({
    dagId,
    dagRunId,
    onSuccessConfirm: onClose,
    onSuccessDryRun: setAffectedTasks,
  });

  return (
    <Box>
      <ActionButton
        actionName="Clear Task Instance"
        icon={<FiRefreshCw />}
        onClick={() => {
          onOpen();
          mutate({
            dagId,
            requestBody: {
              dag_run_id: dagRunId,
              dry_run: true,
              include_downstream: downstream,
              include_future: future,
              include_past: past,
              include_upstream: upstream,
              only_failed: onlyFailed,
              task_ids: [taskInstance.task_id],
            },
          });
        }}
        text="Clear Task Instance"
        withText={withText}
      />

      <ClearTaskInstanceDialog
        affectedTasks={affectedTasks}
        downstream={downstream}
        future={future}
        isPending={isPending}
        mutate={mutate}
        onClose={onClose}
        onlyFailed={onlyFailed}
        onToggleDownstream={onToggleDownstream}
        onToggleFuture={onToggleFuture}
        onToggleOnlyFailed={onToggleOnlyFailed}
        onTogglePast={onTogglePast}
        onToggleUpstream={onToggleUpstream}
        open={open}
        past={past}
        taskInstance={taskInstance}
        upstream={upstream}
      />
    </Box>
  );
};

export default ClearTaskInstanceButton;
