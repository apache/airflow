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
import { Button, Flex } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import type { TaskInstanceResponse, TaskInstanceState } from "openapi/requests/types.gen";
import { ActionAccordion } from "src/components/ActionAccordion";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { useMarkTaskInstanceDefaultOptions } from "src/hooks/useUserSettings";
import { usePatchTaskInstance } from "src/queries/usePatchTaskInstance";
import { usePatchTaskInstanceDryRun } from "src/queries/usePatchTaskInstanceDryRun";
import { Modal, SegmentedControl } from "src/system-components";

type Props = {
  readonly onClose: () => void;
  readonly open: boolean;
  readonly state: TaskInstanceState;
  readonly taskInstance: TaskInstanceResponse;
};

const MarkTaskInstanceAsDialog = ({ onClose, open, state, taskInstance }: Props) => {
  const dagId = taskInstance.dag_id;
  const dagRunId = taskInstance.dag_run_id;
  const taskId = taskInstance.task_id;
  const mapIndex = taskInstance.map_index;
  const { t: translate } = useTranslation();

  const [markTaskInstanceDefaultOptions] = useMarkTaskInstanceDefaultOptions();
  const [selectedOptions, setSelectedOptions] = useState<Array<string>>(markTaskInstanceDefaultOptions);

  const past = selectedOptions.includes("past");
  const future = selectedOptions.includes("future");
  const upstream = selectedOptions.includes("upstream");
  const downstream = selectedOptions.includes("downstream");

  const [note, setNote] = useState<string | null>(taskInstance.note);

  useEffect(() => {
    if (open) {
      setNote(taskInstance.note);
    }
  }, [open, taskInstance.note]);

  const handleClose = () => {
    setNote(taskInstance.note);
    onClose();
  };

  const { isPending, mutate } = usePatchTaskInstance({
    dagId,
    dagRunId,
    mapIndex,
    onSuccess: handleClose,
    taskId,
  });
  const { data, isPending: isPendingDryRun } = usePatchTaskInstanceDryRun({
    dagId,
    dagRunId,
    mapIndex,
    options: {
      enabled: open,
      refetchOnMount: "always",
    },
    requestBody: {
      include_downstream: downstream,
      include_future: future,
      include_past: past,
      include_upstream: upstream,
      new_state: state,
      note,
    },
    taskId,
  });

  const affectedTasks = data ?? {
    task_instances: [],
    total_entries: 0,
  };

  return (
    <Modal
      footerActions={
        <Button
          loading={isPending || isPendingDryRun}
          onClick={() => {
            mutate({
              dagId,
              dagRunId,
              mapIndex,
              requestBody: {
                include_downstream: downstream,
                include_future: future,
                include_past: past,
                include_upstream: upstream,
                new_state: state,
                note,
              },
              taskId,
            });
          }}
        >
          {translate("modal.confirm")}
        </Button>
      }
      lazyMount
      onOpenChange={(details) => {
        if (!details.open) {
          handleClose();
        }
      }}
      open={open}
      title={
        <>
          {translate("dags:runAndTaskActions.markAs.title", {
            state,
            type: translate("taskInstance_one"),
          })}
          : {taskInstance.task_display_name} <Time datetime={taskInstance.start_date} />{" "}
          <StateBadge state={state} />
        </>
      }
    >
      <Flex justifyContent="center">
        <SegmentedControl
          defaultValues={markTaskInstanceDefaultOptions}
          multiple
          onChange={setSelectedOptions}
          options={[
            {
              disabled: taskInstance.logical_date === null,
              label: translate("dags:runAndTaskActions.options.past"),
              value: "past",
            },
            {
              disabled: taskInstance.logical_date === null,
              label: translate("dags:runAndTaskActions.options.future"),
              value: "future",
            },
            {
              label: translate("dags:runAndTaskActions.options.upstream"),
              value: "upstream",
            },
            {
              label: translate("dags:runAndTaskActions.options.downstream"),
              value: "downstream",
            },
          ]}
        />
      </Flex>
      <ActionAccordion affectedTasks={affectedTasks} note={note} setNote={setNote} />
    </Modal>
  );
};

export default MarkTaskInstanceAsDialog;
